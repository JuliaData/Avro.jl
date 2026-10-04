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
    schema::Schema
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

"A resolving enum remap decoded directly to `String` or `Symbol`."
struct EnumRemapTarget{T} <: TypedPlan
    plan::EnumRemapPlan
end

"A resolving enum remap decoded to a `Base.Enum` by reader symbol."
struct EnumRemapNativeTarget{T} <: TypedPlan
    plan::EnumRemapPlan
    members::Vector{Union{Nothing,T}}
end

struct ArrayTarget{E,P<:TypedPlan} <: TypedPlan
    items::P
    minsize::Int
    inlineshell::Int
end

"An exactly accounted `Avro.Map{V}` target. `Dict` uses `SemanticTarget`."
struct MapTarget{K,V,P<:TypedPlan} <: TypedPlan
    values::P
    minsize::Int
    dict::Bool
    inlineshell::Int
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
    used::Bool
end

# ---- construction -----------------------------------------------------------------------------------

"One mutable memo slot. Inactive slots retain abandoned graphs only until construction ends."
mutable struct TypedMemoEntry
    target::Any
    plan::Union{Nothing,TypedPlan}
    active::Bool
end

"A checkpoint for both unsettled graph storage and speculative memo changes."
struct TypedMark
    fresh::Int
    changes::Int
end

"""
Per-node (target type => plan) entries, no hashing, plus the construction budget (round-2 D06).
`fresh` tracks all settled storage not yet committed to a returned graph or an outer fallback frame.
The paired change buffers make nested record, union and resolution attempts transactional. Rollback
clears every discarded plan reference before its graph storage is released, so a later attempt cannot
reuse released storage and abandoned candidates do not consume later headroom (round-5 item 1).
"""
struct TypedMemo
    entries::Vector{Vector{TypedMemoEntry}}
    budget::Budget
    fresh::Base.RefValue{Int}
    specdepth::Base.RefValue{Int}
    changes::BuildBuf{TypedMemoEntry}
    replaced::BuildBuf{Union{Nothing,TypedMemoEntry}}
end

"Make memo writes after `mark` invisible, restoring entries that those writes replaced."
function rollbackmemo!(memo::TypedMemo, mark::TypedMark)
    for i in memo.changes.len:-1:(mark.changes + 1)
        addresolution!(memo.budget)
        added = @inbounds memo.changes.data[i]
        prior = @inbounds memo.replaced.data[i]
        added.active = false
        added.plan = nothing
        prior === nothing || (prior.active = true)
    end
    memo.changes.len = mark.changes
    memo.replaced.len = mark.changes
    return nothing
end

"Release graph storage since `mark` after rollback has cleared every memo reference to it."
function releasefresh!(memo::TypedMemo, mark::TypedMark)
    delta = memo.fresh[] - mark.fresh
    delta <= 0 && return nothing
    release!(memo.budget, delta)
    memo.fresh[] = mark.fresh
    return nothing
end

"Start a speculative typed-plan frame and checkpoint graph storage plus memo writes."
function beginfresh!(memo::TypedMemo)
    mark = TypedMark(memo.fresh[], memo.changes.len)
    memo.specdepth[] += 1
    return mark
end

"Commit a successful frame to its parent, or to the returned graph at the outer boundary."
function keepfresh!(memo::TypedMemo, mark::TypedMark)
    memo.specdepth[] -= 1
    memo.specdepth[] >= 0 || throw(ArgumentError("typed-plan speculation depth became negative"))
    if memo.specdepth[] == 0
        memo.fresh[] = mark.fresh
        memo.changes.len = 0
        memo.replaced.len = 0
    end
    return nothing
end

"Discard a failed frame and return to its parent frame."
function discardfresh!(memo::TypedMemo, mark::TypedMark)
    rollbackmemo!(memo, mark)
    releasefresh!(memo, mark)
    memo.specdepth[] -= 1
    memo.specdepth[] >= 0 || throw(ArgumentError("typed-plan speculation depth became negative"))
    return nothing
end

"Record already settled storage retained directly by a fresh typed-plan node."
function ownfresh!(memo::TypedMemo, bytes::Int)
    memo.fresh[] += bytes
    return nothing
end

function Base.getindex(m::TypedMemo, i::Int)
    return m.entries[i]
end

"Allocate one exact-capacity typed-plan vector after charging it to the construction scope."
function typedvector(::Type{E}, n::Int, memo::TypedMemo) where {E}
    charge = vectorbytes(E, n)
    reserve!(memo.budget, charge)
    try
        values = Vector{E}(undef, n)
        allocated!(memo.budget, charge)
        return values
    catch
        unreserve!(memo.budget, charge)
        rethrow()
    end
end

"""
    typedplan(T, reader_schema, plan, limits) -> TypedPlan

The typed plan decoding `plan` (the generic or resolving plan of `reader_schema`) into `T`.
"""
function typedplan(::Type{T}, reader::Schema, plan::ReadPlan, limits::Limits;
                   budget::Union{Nothing,Budget}=nothing) where {T}
    if budget === nothing
        return withbudget(limits) do operation_budget
            return typedplan(T, reader, plan, limits; budget=operation_budget)
        end
    end
    checkpoint = budgetcheckpoint(budget)
    try
        root, dead = buildtypedroot(T, reader, plan, budget)
        release!(budget, dead)
        return root
    catch
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

"Build a typed plan and return its construction-only memo charge after the memo frame has unwound."
function buildtypedroot(::Type{T}, reader::Schema, plan::ReadPlan, budget::Budget) where {T}
    nodes = graphinfo(reader).nodes
    addresolution!(budget, nodes)                    # initialise one memo slot per dense schema node
    memocharge = checked_add(vectorbytes(Vector{TypedMemoEntry}, nodes),
                             checked_mul(STORAGE[].vector, nodes))
    reserve!(budget, memocharge)
    entries = Vector{Vector{TypedMemoEntry}}(undef, nodes)
    for i in eachindex(entries)
        entries[i] = TypedMemoEntry[]
    end
    allocated!(budget, memocharge)
    changes = BuildBuf{TypedMemoEntry}(budget, 0)
    replaced = BuildBuf{Union{Nothing,TypedMemoEntry}}(budget, 0)
    memo = TypedMemo(entries, budget, Ref(0), Ref(0), changes, replaced)
    root = buildtyped(T, reader, plan, memo)
    # The memo table, its entry shells and rollback buffers are construction-only. Discarded entries have
    # already cleared their plan references, so only these container allocations die at this boundary.
    # Memoised nodes stay charged — the plan graph (or a recursive reference) may hold them (item 4).
    dead = vectorbytes(Vector{TypedMemoEntry}, nodes)
    for v in entries
        dead = checked_add(dead, vectorbytes(TypedMemoEntry, length(v)) + memoentrybytes() * length(v))
    end
    dead = checked_add(dead, vectorbytes(TypedMemoEntry, length(changes.data)))
    dead = checked_add(dead, vectorbytes(Union{Nothing,TypedMemoEntry}, length(replaced.data)))
    dead = checked_add(dead, shellbytes(typeof(changes)) + shellbytes(typeof(replaced)))
    return (root, dead)
end

function memolookup(memo::TypedMemo, s::Schema, ::Type{T}) where {T}
    entries = memo[Int(nodeid(s)) + 1]
    for i in length(entries):-1:1
        addresolution!(memo.budget)
        entry = @inbounds entries[i]
        if entry.active && entry.target === T
            p = entry.plan::TypedPlan
            p isa RefTarget && (p.used = true)
            return p
        end
    end
    return nothing
end

function memoentrybytes()
    return shellbytes(TypedMemoEntry)
end

"Append one entry through exact-capacity replacement."
function appendmemo!(memo::TypedMemo, slot::Int, entry::TypedMemoEntry)
    entries = memo[slot]
    n = checked_add(length(entries), 1)
    addresolution!(memo.budget, n)                   # retained entries copied plus the appended entry
    newcharge = vectorbytes(TypedMemoEntry, n)
    reserve!(memo.budget, newcharge)
    replacement = Vector{TypedMemoEntry}(undef, n)
    allocated!(memo.budget, newcharge)
    copyto!(replacement, 1, entries, 1, n - 1)
    replacement[n] = entry
    oldcharge = vectorbytes(TypedMemoEntry, n - 1)
    memo.entries[slot] = replacement
    entries = nothing
    release!(memo.budget, oldcharge)
    return entry
end

function activeentry(entries::Vector{TypedMemoEntry}, ::Type{T}, budget::Budget) where {T}
    for i in length(entries):-1:1
        addresolution!(budget)
        entry = @inbounds entries[i]
        entry.active && entry.target === T && return entry
    end
    return nothing
end

function memostore!(memo::TypedMemo, s::Schema, ::Type{T}, p::TypedPlan) where {T}
    slot = Int(nodeid(s)) + 1
    entries = memo[slot]
    prior = activeentry(entries, T, memo.budget)
    if memo.specdepth[] == 0
        charge = nodebytes(typeof(p))
        memo.fresh[] >= charge || throw(ArgumentError("typed-plan memo stored $charge untracked node bytes"))
        memo.fresh[] -= charge                         # committed memo ownership outside a fallback frame
        if prior !== nothing
            addresolution!(memo.budget)
            prior.plan = p
            return p
        end
    end
    reserve!(memo.budget, memoentrybytes())
    entry = TypedMemoEntry(T, p, true)
    allocated!(memo.budget, memoentrybytes())
    if memo.specdepth[] > 0
        typedmemopush!(memo.changes, entry, memo.budget)
        typedmemopush!(memo.replaced, prior, memo.budget)
    end
    appendmemo!(memo, slot, entry)
    if prior !== nothing
        addresolution!(memo.budget)
        prior.active = false
    end
    return p
end

function typedmemopush!(buffer::BuildBuf, value, budget::Budget)
    buffer.len == length(buffer.data) && addresolution!(budget, buffer.len)
    addresolution!(budget)
    push!(buffer, budget, value)
    return nothing
end

# ---- construction accounting (plan §4.4 (e), round-3 item 4) -----------------------------------------
#
# Every typed-plan node is charged to the construction scope before it is built. A node's box charge is
# the `shellbytes` convention over its concrete type (an isbits node charges its box, since it lives
# boxed behind the abstract `TypedPlan` interface). A freshly built immutable node captured inline by
# its parent — a concrete immutable field or tuple element — has its transient box released after the
# capture (the parent's own charge covers the inline copy). Abandoned fallback nodes stay charged until
# the operation ends: they may be memo-referenced, so their boxes cannot be released individually.

"The box charge of one typed-plan node of concrete type `P`, made before the node is built."
function nodebytes(::Type{P}) where {P}
    return isbitstype(P) ? boxbytes(P) : shellbytes(P)
end

"Reserve one node's box before it is built; `settlenode!` settles it right after construction (§4.4)."
function reservenode!(memo::TypedMemo, ::Type{P}) where {P}
    addresolution!(memo.budget)
    reserve!(memo.budget, nodebytes(P))
    return nothing
end

function settlenode!(memo::TypedMemo, ::Type{P}) where {P}
    allocated!(memo.budget, nodebytes(P))
    memo.fresh[] += nodebytes(P)
    return nothing
end

"Release the transient box of a freshly built node its parent just captured inline (mutable: a reference, nothing to release)."
function releasecapture!(memo::TypedMemo, node)
    ismutabletype(typeof(node)) && return nothing
    release!(memo.budget, nodebytes(typeof(node)))
    memo.fresh[] -= nodebytes(typeof(node))
    return nothing
end

"""
Build the tuple `(parts...,)` under the construction scope: a bound over the inline element sizes is
reserved first, the actual box charge is settled and the slack returned. Returns `(tuple, boxbytes)`;
the caller releases `boxbytes` once the tuple's owner is built (tuples used as type parameters are
interned by the type system, which is process-global and outside operation accounting).
"""
function chargedtuple(memo::TypedMemo, parts::Vector)
    addresolution!(memo.budget, length(parts))
    bound = 16
    for x in parts
        bound += ismutabletype(typeof(x)) ? 8 : max(sizeof(typeof(x)), 8) + 16
    end
    reserve!(memo.budget, bound)
    ps = (parts...,)
    actual = 16 + sizeof(typeof(ps))
    actual <= bound || throw(ArgumentError("tuple storage $actual exceeds its construction bound $bound"))
    allocated!(memo.budget, actual)
    unreserve!(memo.budget, bound - actual)
    return (ps, actual)
end

function chargedgeneric(p::ReadPlan, memo::TypedMemo)
    reservenode!(memo, GenericTarget{typeof(p)})
    g = GenericTarget(p)
    settlenode!(memo, GenericTarget{typeof(p)})
    return g
end

function chargedsemantic(::Type{T}, s::Schema, p::ReadPlan, memo::TypedMemo) where {T}
    reservenode!(memo, SemanticTarget{T})
    t = SemanticTarget{T}(p, s)
    settlenode!(memo, SemanticTarget{T})
    return t
end

function buildtyped(::Type{T}, s::Schema, p::ReadPlan, memo::TypedMemo) where {T}
    addresolution!(memo.budget)                       # one target/schema match attempt
    T === Any && return chargedgeneric(p, memo)
    T === juliatype(s) && return chargedgeneric(p, memo)
    customhooks(T) && return chargedsemantic(T, s, p, memo)
    if isresolving(p)                                        # the settled eligibility analysis applies to
        mark = beginfresh!(memo)
        t = buildresolvedtyped(T, s, p, memo)                # resolved plans too (plan §4.8, R18); anything
        if t !== nothing
            keepfresh!(memo, mark)
            return t
        end
        discardfresh!(memo, mark)                            # the abandoned attempt's complete storage dies here
        return chargedsemantic(T, s, p, memo)
    end
    s isa UnionSchema && return builduniontarget(T, s, p::UnionPlan, memo)
    if T isa Union
        _, inner = splitoptional(T)
        inner === nothing && return chargedsemantic(T, s, p, memo)
        return buildtyped(inner, s, p, memo)      # a non-union schema never produces the null member
    end
    s isa RecordSchema && return buildrecordtarget(T, s, p::RecordPlan, memo)
    s isa ArraySchema && return buildarraytarget(T, s, p::ArrayPlan, memo)
    s isa MapSchema && return buildmaptarget(T, s, p::MapPlan, memo)
    return buildleaftarget(T, s, p, memo)
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
    frame = beginfresh!(memo)
    nb = p.nullable
    if nb != 0
        other, op = s.branches[3 - nb], p.branches[3 - nb]
        N, inner = T isa Union ? splitoptional(T) : (Union{}, T)
        if inner === nothing
            discardfresh!(memo, frame)
            return chargedsemantic(T, s, p, memo)
        end
        ip = buildtyped(inner, other, op, memo)
        if ip isa SemanticTarget
            discardfresh!(memo, frame)
            return chargedsemantic(T, s, p, memo)
        end
        reservenode!(memo, NullableTarget{N,typeof(ip)})
        out = NullableTarget{N,typeof(ip)}(ip, nb)
        settlenode!(memo, NullableTarget{N,typeof(ip)})
        releasecapture!(memo, ip)
        keepfresh!(memo, frame)
        return out
    end
    rawmembers = T isa Union ? Base.uniontypes(T) : (T,)
    memberscharge = vectorbytes(Any, length(rawmembers))
    members = typedvector(Any, length(rawmembers), memo)
    for i in eachindex(rawmembers)
        members[i] = rawmembers[i]
    end
    branchescharge = vectorbytes(TypedPlan, length(s.branches))
    branches = typedvector(TypedPlan, length(s.branches), memo)
    for (i, (b, bp)) in enumerate(zip(s.branches, p.branches))
        found = nothing
        for m in members
            mark = beginfresh!(memo)
            tp = buildtyped(m, b, bp, memo)
            if tp isa SemanticTarget
                discardfresh!(memo, mark)              # the failed attempt's complete subtree is abandoned
                continue
            end
            keepfresh!(memo, mark)
            found = tp
            break
        end
        if found === nothing
            release!(memo.budget, memberscharge + branchescharge)   # both scratch vectors die with the fallback
            discardfresh!(memo, frame)
            return chargedsemantic(T, s, p, memo)
        end
        branches[i] = found
    end
    release!(memo.budget, memberscharge)               # the member-type scratch dies here; `branches` is retained
    reservenode!(memo, UnionTarget{T})
    out = UnionTarget{T}(branches)
    settlenode!(memo, UnionTarget{T})
    ownfresh!(memo, branchescharge)
    keepfresh!(memo, frame)
    return out
end

function buildleaftarget(::Type{T}, s::Schema, p::ReadPlan, memo::TypedMemo) where {T}
    if T === Symbol && p isa Union{StringPlan,EnumPlan}
        reservenode!(memo, SymbolTarget{typeof(p)})
        out = SymbolTarget(p)
        settlenode!(memo, SymbolTarget{typeof(p)})
        return out
    end
    T <: Base.Enum && p isa EnumPlan && return enumtarget(T, p, memo)
    if leafcompatible(T, p)
        reservenode!(memo, LeafTarget{T,typeof(p)})
        out = LeafTarget{T,typeof(p)}(p)
        settlenode!(memo, LeafTarget{T,typeof(p)})
        return out
    end
    return chargedsemantic(T, s, p, memo)
end

function enumtarget(::Type{T}, p::EnumPlan, memo::TypedMemo) where {T<:Base.Enum}
    syms = p.schema.symbols
    members = typedvector(Union{Nothing,T}, length(syms), memo)
    fill!(members, nothing)
    for e in instances(T)
        name = avrosymbol(T, e)
        haskey(p.schema.symbolindex, name) || continue
        members[p.schema.symbolindex[name]] = e
    end
    reservenode!(memo, EnumTarget{T})
    out = EnumTarget{T}(p, members)
    settlenode!(memo, EnumTarget{T})
    ownfresh!(memo, vectorbytes(Union{Nothing,T}, length(syms)))
    return out
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

function leafcompatible(::Type{T}, ::EnumPlan) where {T}
    return T === String
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

function buildrecordtarget(::Type{T}, s::RecordSchema, p::RecordPlan, memo::TypedMemo) where {T}
    cached = memolookup(memo, s, T)
    cached === nothing || return cached
    fastroute(T) || return memostore!(memo, s, T, chargedsemantic(T, s, p, memo))
    reservenode!(memo, RefTarget{T})
    ref = RefTarget{T}(nothing, false)
    settlenode!(memo, RefTarget{T})
    reffresh = memo.specdepth[] > 0
    memostore!(memo, s, T, ref)
    mark = beginfresh!(memo)
    built = buildrecordplan(T, s, p, memo)
    if built === nothing
        discardfresh!(memo, mark)                      # the abandoned field plans and owned vectors die
        # Recursive references captured `ref` already: they convert semantically where they occur.
        sem = chargedsemantic(T, s, p, memo)
        ref.plan = sem
        out = memostore!(memo, s, T, sem)
        abandonunusedref!(memo, s, ref, reffresh)
        return out
    end
    ref.plan = built
    out = memostore!(memo, s, T, built)
    keepfresh!(memo, mark)
    abandonunusedref!(memo, s, ref, reffresh)
    return out
end

"Clear and release an unused recursive placeholder after its memo slot has been replaced."
function abandonunusedref!(memo::TypedMemo, s::Schema, ref::RefTarget{T}, fresh::Bool) where {T}
    ref.used && return nothing
    charge = nodebytes(RefTarget{T})
    for entry in memo[Int(nodeid(s)) + 1]
        entry.plan === ref && (entry.plan = nothing)
    end
    ref.plan = nothing
    release!(memo.budget, charge)
    fresh && (memo.fresh[] -= charge)
    return nothing
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

function budgetedavrofieldname(tags, field::Symbol, budget::Budget)
    tagged = fieldtag(tags, field, :name)
    raw = tagged === nothing ? field : tagged
    raw isa Union{Symbol,AbstractString} ||
        throw(ArgumentError("StructUtils field tag `name` must be a Symbol or string, got $(typeof(raw))"))
    n = sizeof(raw)
    n <= budget.limits.max_name_bytes ||
        throw(limiterror(budget, :max_name_bytes, n, budget.limits.max_name_bytes))
    return ownedstringcopy(raw, budget)
end

function findtypedfield(name::String, candidates::Vector{String}, budget::Budget)
    for i in eachindex(candidates)
        addresolution!(budget)
        candidates[i] == name && return i
    end
    return nothing
end

function resolvedfieldindex(index::FrozenDict{String,Int}, name::String, budget::Budget)
    keys = index.keys
    lo = 1
    hi = length(keys) + 1
    while lo < hi
        mid = lo + ((hi - lo) >>> 1)
        addresolution!(budget)
        addcompare!(budget, keycomparisonwork(keys[mid], name))
        if isless(keys[mid], name)
            lo = mid + 1
        else
            hi = mid
        end
    end
    if lo <= length(keys)
        addresolution!(budget)
        addcompare!(budget, keycomparisonwork(keys[lo], name))
        keys[lo] == name && return index.vals[lo]
    end
    return 0
end

function buildrecordplan(::Type{T}, s::RecordSchema, p::RecordPlan, memo::TypedMemo) where {T}
    names = fieldnames(T)
    tags = StructUtils.fieldtags(AvroStyle(), T)
    scratch = vectorbytes(String, length(names))       # construction-only storage, released on every exit
    avronames = typedvector(String, length(names), memo)
    for i in eachindex(names)
        avronames[i] = budgetedavrofieldname(tags, names[i], memo.budget)
        scratch += stringbytes(sizeof(avronames[i]))
    end
    nschema = length(s.fields)
    scratch += vectorbytes(TypedPlan, nschema) + vectorbytes(Int, length(names))
    plans = typedvector(TypedPlan, nschema, memo)
    map = typedvector(Int, length(names), memo)
    fill!(map, 0)
    for (j, f) in enumerate(s.fields)
        k = findtypedfield(f.name, avronames, memo.budget)
        if k === nothing
            reservenode!(memo, SkipTarget{typeof(p.fields[j])})
            plans[j] = SkipTarget(p.fields[j])
            settlenode!(memo, SkipTarget{typeof(p.fields[j])})
            continue
        end
        tp = buildtyped(fieldtype(T, k), f.schema, p.fields[j], memo)
        tp isa SemanticTarget && (release!(memo.budget, scratch); return nothing)
        plans[j] = tp
        map[k] = j
    end
    defaults = typedvector(Any, length(names), memo)
    defs = StructUtils.fielddefaults(AvroStyle(), T)
    defaultcharge = 0
    for k in eachindex(names)
        map[k] == 0 || continue
        ft = fieldtype(T, k)
        if haskey(defs, names[k])
            v = defs[names[k]]
            if !(v isa ft)
                release!(memo.budget, scratch + vectorbytes(Any, length(names)) + defaultcharge)   # `defaults` dies with the fallback
                return nothing
            end
            bb = isbits(v) ? boxbytes(typeof(v)) : 0
            bb > 0 && reserve!(memo.budget, bb)        # the retained Any-slot box, made at the assignment
            defaults[k] = v
            bb > 0 && allocated!(memo.budget, bb)
            defaultcharge += bb
        elseif Missing <: ft
            defaults[k] = missing
        elseif Nothing <: ft
            defaults[k] = nothing
        else
            throw(ArgumentError(typediagnostic(
                memo.budget, "target type ", T, " has field ", boundedquoted(names[k]),
                " with no field ", boundedquoted(avronames[k]),
                " in the reader record and no default")))
        end
    end
    ps, psbox = chargedtuple(memo, plans)
    for x in plans                                     # freshly built immutable nodes now live inline in the tuple
        releasecapture!(memo, x)
    end
    mp, mpbox = chargedtuple(memo, map)
    reservenode!(memo, RecordTarget{T,typeof(ps),mp})
    out = RecordTarget{T,typeof(ps),mp}(s, ps, defaults, measuredshell(T, memo.budget))
    settlenode!(memo, RecordTarget{T,typeof(ps),mp})
    ownfresh!(memo, vectorbytes(Any, length(names)) + defaultcharge)
    release!(memo.budget, psbox + mpbox + scratch)     # the tuple boxes and construction-only scratch die here
    return out
end

"""
The direct typed route over resolving plans (plan §4.8, R18): promotions and enum remaps are leaves
(their decode already yields the reader value); a two-branch-nullable reader union resolves through a
wrapped or per-writer-branch target; resolved records decode writer-ordered steps straight into `T`'s
fields with reader-only defaults materialised once at plan time. Returns `nothing` when ineligible.
"""
function buildresolvedtyped(::Type{T}, s::Schema, p::ReadPlan, memo::TypedMemo) where {T}
    p isa PromotePlan && return buildleaftarget(T, s, p, memo)
    p isa EnumRemapPlan && return buildenumremaptarget(T, p, memo)
    if p isa WrapPlan && p.nullable != 0 && s isa UnionSchema
        N, inner = T isa Union ? splitoptional(T) : (Union{}, T)
        inner === nothing && return nothing
        if p.readerindex == p.nullable
            N === Union{} && return nothing
            reservenode!(memo, ResolvedNullTarget{N,typeof(p.inner)})
            out = ResolvedNullTarget{N,typeof(p.inner)}(p.inner)
            settlenode!(memo, ResolvedNullTarget{N,typeof(p.inner)})
            return out
        end
        return buildtyped(inner, s.branches[p.readerindex], p.inner, memo)   # the writer never encodes null here
    end
    if p isa UnionResolvePlan && p.nullable != 0 && s isa UnionSchema
        N, inner = T isa Union ? splitoptional(T) : (Union{}, T)
        (inner === nothing || N === Union{}) && return nothing
        branchescharge = vectorbytes(TypedPlan, length(p.branches))
        branches = typedvector(TypedPlan, length(p.branches), memo)
        isnull = typedvector(Bool, length(p.branches), memo)
        for (i, bp) in enumerate(p.branches)
            if p.readerindex[i] == p.nullable
                branches[i] = chargedgeneric(bp, memo)                       # decodes missing
                isnull[i] = true
            else
                bt = bp isa UnresolvableBranch ? chargedgeneric(bp, memo) :
                     buildtyped(inner, s.branches[p.readerindex[i]], bp, memo)
                if bt isa SemanticTarget
                    release!(memo.budget, branchescharge + vectorbytes(Bool, length(p.branches)))   # both scratch vectors die
                    return nothing
                end
                branches[i] = bt
                isnull[i] = false
            end
        end
        bs, bsbox = chargedtuple(memo, branches)
        for x in branches                              # freshly built immutable nodes now live inline in the tuple
            releasecapture!(memo, x)
        end
        reservenode!(memo, ResolvedNullableTarget{N === Missing ? Missing : Nothing,typeof(bs)})
        out = ResolvedNullableTarget{N === Missing ? Missing : Nothing,typeof(bs)}(bs, isnull)
        settlenode!(memo, ResolvedNullableTarget{N === Missing ? Missing : Nothing,typeof(bs)})
        ownfresh!(memo, vectorbytes(Bool, length(p.branches)))
        release!(memo.budget, bsbox + branchescharge)  # the tuple box and branch scratch die here; `isnull` is retained
        return out
    end
    p isa ResolvedRecordPlan && s isa RecordSchema && return buildresolvedrecord(T, s, p, memo)
    return nothing
end

function buildenumremaptarget(::Type{T}, p::EnumRemapPlan, memo::TypedMemo) where {T}
    if T === String
        reservenode!(memo, EnumRemapTarget{String})
        out = EnumRemapTarget{String}(p)
        settlenode!(memo, EnumRemapTarget{String})
        return out
    end
    if T === Symbol
        reservenode!(memo, EnumRemapTarget{Symbol})
        out = EnumRemapTarget{Symbol}(p)
        settlenode!(memo, EnumRemapTarget{Symbol})
        return out
    end
    T <: Base.Enum || return nothing
    members = typedvector(Union{Nothing,T}, length(p.reader.symbols), memo)
    fill!(members, nothing)
    for e in instances(T)
        name = avrosymbol(T, e)
        haskey(p.reader.symbolindex, name) || continue
        members[p.reader.symbolindex[name]] = e
    end
    reservenode!(memo, EnumRemapNativeTarget{T})
    out = EnumRemapNativeTarget{T}(p, members)
    settlenode!(memo, EnumRemapNativeTarget{T})
    ownfresh!(memo, vectorbytes(Union{Nothing,T}, length(p.reader.symbols)))
    return out
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
        skipnothing = decodetyped(p.branches[i], d, names)                    # consumes the writer branch (null: nothing)
        return N === Missing ? missing : nothing
    end
    return decodetyped(p.branches[i], d, names)
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
    fastroute(T) || return memostore!(memo, s, T, chargedsemantic(T, s, p, memo))
    reservenode!(memo, RefTarget{T})
    ref = RefTarget{T}(nothing, false)
    settlenode!(memo, RefTarget{T})
    reffresh = memo.specdepth[] > 0
    memostore!(memo, s, T, ref)
    mark = beginfresh!(memo)
    built = buildresolvedrecordplan(T, s, p, memo)
    if built === nothing
        discardfresh!(memo, mark)                      # the abandoned step plans and owned vectors die
        sem = chargedsemantic(T, s, p, memo)
        ref.plan = sem
        out = memostore!(memo, s, T, sem)
        abandonunusedref!(memo, s, ref, reffresh)
        return out
    end
    ref.plan = built
    out = memostore!(memo, s, T, built)
    keepfresh!(memo, mark)
    abandonunusedref!(memo, s, ref, reffresh)
    return out
end

function resolvedslotmap(::Type{T}, s::RecordSchema, memo::TypedMemo) where {T}
    names = fieldnames(T)
    tags = StructUtils.fieldtags(AvroStyle(), T)
    namescharge = vectorbytes(String, length(names))   # construction-only: released before returning
    avronames = typedvector(String, length(names), memo)
    for i in eachindex(names)
        avronames[i] = budgetedavrofieldname(tags, names[i], memo.budget)
        namescharge += stringbytes(sizeof(avronames[i]))
    end
    slotfor = typedvector(Int, length(s.fields), memo)    # reader slot -> T field
    fill!(slotfor, 0)
    for (k, an) in enumerate(avronames)
        i = resolvedfieldindex(s.fieldindex, an, memo.budget)
        i == 0 && continue
        slotfor[i] = k
    end
    release!(memo.budget, namescharge)                 # the name copies die here; the caller owns `slotfor`
    return (names, slotfor)
end

function resolvedsteptargets(::Type{T}, s::RecordSchema, p::ResolvedRecordPlan, memo::TypedMemo,
                             slotfor::Vector{Int}) where {T}
    plans = typedvector(TypedPlan, length(p.steps), memo)
    stepslot = typedvector(Int, length(p.steps), memo)
    for (i, (slot, sp)) in enumerate(p.steps)
        k = slot == 0 ? 0 : slotfor[slot]
        if k == 0
            reservenode!(memo, SkipTarget{typeof(sp)})
            plans[i] = SkipTarget(sp)
            settlenode!(memo, SkipTarget{typeof(sp)})
            stepslot[i] = 0
        else
            tp = buildtyped(fieldtype(T, k), s.fields[slot].schema, sp, memo)
            if tp isa SemanticTarget
                release!(memo.budget, vectorbytes(TypedPlan, length(p.steps)) + vectorbytes(Int, length(p.steps)))
                return nothing
            end
            plans[i] = tp
            stepslot[i] = k
        end
    end
    return (plans, stepslot)
end

function checkedreaderdefault(::Type{T}, dp::DefaultPlan, budget::Budget) where {T}
    checkpoint = budgetcheckpoint(budget)
    valid = false
    v = nothing
    v2 = nothing
    try
        v = jsonvalue(dp, budget; countdatum=false)
        v2 = v isa T ? v : try
            convertleaf(T, v, budget)
        catch caught
            caught isa Union{MethodError,ConversionError} || rethrow()
            nothing
        end
        valid = v2 isa T
    finally
        v = nothing
        v2 = nothing
        rollbackreservations!(budget, checkpoint)
    end
    valid || return nothing
    addresolution!(budget)
    reserve!(budget, nodebytes(ResolvedReaderDefault{T}))
    out = ResolvedReaderDefault{T}(dp)
    allocated!(budget, nodebytes(ResolvedReaderDefault{T}))   # the retained default node
    return out
end

"Build a retained reader default and add its box to the current fallback ledger."
function checkedreaderdefault(::Type{T}, dp::DefaultPlan, memo::TypedMemo) where {T}
    out = checkedreaderdefault(T, dp, memo.budget)
    out === nothing || (memo.fresh[] += nodebytes(ResolvedReaderDefault{T}))
    return out
end

function resolvedreaderdefaults!(::Type{T}, defaults::Vector{Any}, covered::Vector{Bool},
                                 slotfor::Vector{Int}, p::ResolvedRecordPlan, memo::TypedMemo) where {T}
    for (slot, dp) in p.defaults
        k = slotfor[slot]
        k == 0 && continue
        dflt = checkedreaderdefault(fieldtype(T, k), dp, memo)
        dflt === nothing && return false
        defaults[k] = dflt
        covered[k] = true
    end
    return true
end

function resolvedstaticdefaults!(::Type{T}, defaults::Vector{Any}, covered::Vector{Bool}, names,
                                 budget::Budget, retained::Base.RefValue{Int}) where {T}
    defs = StructUtils.fielddefaults(AvroStyle(), T)
    for k in eachindex(names)
        covered[k] && continue
        ft = fieldtype(T, k)
        if haskey(defs, names[k])
            v = defs[names[k]]
            v isa ft || return false
            bb = isbits(v) ? boxbytes(typeof(v)) : 0
            bb > 0 && reserve!(budget, bb)             # the retained Any-slot box, made at the assignment
            defaults[k] = v
            bb > 0 && allocated!(budget, bb)
            retained[] += bb
        elseif Missing <: ft
            defaults[k] = missing
        elseif Nothing <: ft
            defaults[k] = nothing
        else
            throw(ArgumentError(typediagnostic(
                budget, "target type ", T, " has field ", boundedquoted(names[k]),
                " with no writer field, reader default, or static default")))
        end
    end
    return true
end

function buildresolvedrecordplan(::Type{T}, s::RecordSchema, p::ResolvedRecordPlan, memo::TypedMemo) where {T}
    names, slotfor = resolvedslotmap(T, s, memo)
    slotforcharge = vectorbytes(Int, length(s.fields))
    targets = resolvedsteptargets(T, s, p, memo, slotfor)
    if targets === nothing
        release!(memo.budget, slotforcharge)
        return nothing
    end
    plans, stepslot = targets
    scratch = slotforcharge + vectorbytes(TypedPlan, length(p.steps)) + vectorbytes(Int, length(p.steps)) +
              vectorbytes(Bool, length(names))         # construction-only storage, released on every exit
    defaults = typedvector(Any, length(names), memo)
    covered = typedvector(Bool, length(names), memo)
    defaultcharge = Ref(0)
    fill!(covered, false)
    for k in stepslot
        k == 0 || (covered[k] = true)
    end
    if !(resolvedreaderdefaults!(T, defaults, covered, slotfor, p, memo) &&
         resolvedstaticdefaults!(T, defaults, covered, names, memo.budget, defaultcharge))
        release!(memo.budget, scratch + vectorbytes(Any, length(names)) + defaultcharge[])   # `defaults` dies with the fallback
        return nothing
    end
    ps, psbox = chargedtuple(memo, plans)
    for x in plans                                     # freshly built immutable nodes now live inline in the tuple
        releasecapture!(memo, x)
    end
    sm, smbox = chargedtuple(memo, stepslot)
    reservenode!(memo, ResolvedRecordTarget{T,typeof(ps),sm})
    out = ResolvedRecordTarget{T,typeof(ps),sm}(s, ps, defaults, measuredshell(T, memo.budget))
    settlenode!(memo, ResolvedRecordTarget{T,typeof(ps),sm})
    ownfresh!(memo, vectorbytes(Any, length(names)) + defaultcharge[])
    release!(memo.budget, psbox + smbox + scratch)     # the tuple boxes and construction-only scratch die here
    return out
end

function resolveddefault(::Type{T}, x::ResolvedReaderDefault{T}, d::Decoder) where {T}
    v = jsonvalue(x.plan, d.budget; depth=satadd(d.depth, 1))
    return v isa T ? v : convertleaf(T, v, d.budget)::T
end

function resolveddefault(::Type{T}, x, d::Decoder) where {T}
    return x::T
end

function typedvalue(p::ResolvedRecordTarget{T}, d::Decoder, names) where {T}
    enter!(d)
    reserve!(d.budget, p.shell)
    v = resolvedrecordvalue(p, d, names)
    allocated!(d.budget, p.shell)                      # the value's shell (inline shells release at their vector slot)
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
    T <: Vector || return chargedsemantic(T, s, p, memo)
    mark = beginfresh!(memo)
    E = eltype(T)
    ip = buildtyped(E, s.items, p.items, memo)
    if ip isa SemanticTarget
        discardfresh!(memo, mark)
        return chargedsemantic(T, s, p, memo)
    end
    # Julia 1.10's storage oracle counts an immutable non-isbits element both in its vector slot and
    # as a value shell. Julia 1.11 corrected that duplication, so only newer versions transfer it.
    inlineshell = VERSION >= v"1.11" && inlinestruct(E) ? measuredinlineshell(E, memo.budget) : 0
    reservenode!(memo, ArrayTarget{E,typeof(ip)})
    out = ArrayTarget{E,typeof(ip)}(ip, p.minsize, inlineshell)
    settlenode!(memo, ArrayTarget{E,typeof(ip)})
    releasecapture!(memo, ip)
    keepfresh!(memo, mark)
    return out
end

function buildmaptarget(::Type{T}, s::MapSchema, p::MapPlan, memo::TypedMemo) where {T}
    T <: Map || return chargedsemantic(T, s, p, memo)
    K, V = String, eltype(T).parameters[2]
    mark = beginfresh!(memo)
    vp = buildtyped(V, s.values, p.values, memo)
    if vp isa SemanticTarget
        discardfresh!(memo, mark)
        return chargedsemantic(T, s, p, memo)
    end
    # The same immutable inline-shell transfer as arrays: the decoded value's shell moves into its
    # exact `Vector{V}` slot on Julia 1.11+ (round-3 item 4).
    inlineshell = VERSION >= v"1.11" && inlinestruct(V) ? measuredinlineshell(V, memo.budget) : 0
    reservenode!(memo, MapTarget{K,V,typeof(vp)})
    out = MapTarget{K,V,typeof(vp)}(vp, p.minsize, false, inlineshell)
    settlenode!(memo, MapTarget{K,V,typeof(vp)})
    releasecapture!(memo, vp)
    keepfresh!(memo, mark)
    return out
end

# ---- decoding ---------------------------------------------------------------------------------------

function fastskipvalues(plan::Union{GenericTarget,SemanticTarget,LeafTarget,
                                    SymbolTarget,EnumTarget,EnumRemapTarget,
                                    EnumRemapNativeTarget}, limits::Limits)
    return fastskipvalues(plan.plan, limits)
end

function fastskipvalues(plan::SkipTarget, limits::Limits)
    return fastskipvalues(plan.plan, limits)
end

function fastskipvalues(::RefTarget, limits::Limits)
    return -1
end

function fastskipvalues(plan::NullableTarget, limits::Limits)
    return fastskipvalues(plan.inner, limits) == 1 ? 2 : -1
end

function fastskipvalues(plan::UnionTarget, limits::Limits)
    isempty(plan.branches) && return -1
    branchvalues = fastskipvalues(first(plan.branches), limits)
    branchvalues >= 0 || return -1
    for index in 2:length(plan.branches)
        fastskipvalues(plan.branches[index], limits) == branchvalues || return -1
    end
    return checked_add(1, branchvalues)
end

function fastskipvalues(::Union{ArrayTarget,MapTarget,RecordTarget}, limits::Limits)
    return -1
end

function fastskipvalues(plan::ResolvedNullTarget, limits::Limits)
    return fastskipvalues(plan.plan, limits)
end

function fastskipvalues(plan::ResolvedNullableTarget, limits::Limits)
    isempty(plan.branches) && return -1
    branchvalues = fastskipvalues(first(plan.branches), limits)
    branchvalues >= 0 || return -1
    for index in 2:length(plan.branches)
        fastskipvalues(plan.branches[index], limits) == branchvalues || return -1
    end
    return checked_add(1, branchvalues)
end

function fastskipvalues(::ResolvedRecordTarget, limits::Limits)
    return -1
end

function syntheticvalue!(plan::Union{GenericTarget,SemanticTarget,LeafTarget,
                                     SymbolTarget,EnumTarget,EnumRemapTarget,
                                     EnumRemapNativeTarget},
                         decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    return syntheticvalue!(plan.plan, decoder, active, counter)
end

function syntheticvalue!(plan::SkipTarget, decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    return syntheticvalue!(plan.plan, decoder, false, counter)
end

function syntheticvalue!(plan::RefTarget, decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    return syntheticvalue!(plan.plan::TypedPlan, decoder, active, counter)
end

function syntheticvalue!(plan::NullableTarget, decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    values = countwriter!(counter)
    index = readindex(decoder, 2)
    index == plan.nullpos && return syntheticvalueadd(counter, values,
                                                      countwriter!(counter))
    return syntheticvalueadd(counter, values,
                             syntheticvalue!(plan.inner, decoder, active,
                                             counter))
end

function syntheticvalue!(plan::UnionTarget, decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    isempty(plan.branches) && dataerror(decoder, "empty union has no datum")
    values = countwriter!(counter)
    index = readindex(decoder, length(plan.branches))
    return syntheticvalueadd(counter, values,
                             syntheticvalue!(plan.branches[index], decoder,
                                             active, counter))
end

function syntheticvalue!(plan::ArrayTarget, decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    enter!(decoder)
    values = countwriter!(counter)
    try
        while true
            count, size = readblockcount(decoder)
            count == 0 && break
            outerstop = decoder.stop
            blocknext = -1
            if size >= 0
                blocknext = checked_add(decoder.pos, size)
                stop = blocknext - 1
                stop <= decoder.stop || dataerror(decoder, "sized array block exceeds the remaining bytes")
                itemvalues = active || !counter.countwriter ? 0 :
                    fastskipvalues(plan.items, decoder.budget.limits)
                if !active && (!counter.countwriter || itemvalues >= 0)
                    skipped = countwriterproduct!(counter, count, itemvalues)
                    values = syntheticvalueadd(counter, values, skipped)
                    decoder.pos = blocknext
                    continue
                end
                decoder.stop = stop
            end
            try
                for _ in 1:count
                    values = syntheticvalueadd(counter, values,
                                               syntheticvalue!(plan.items, decoder,
                                                               active, counter))
                end
                size >= 0 && decoder.pos != blocknext &&
                    dataerror(decoder, "sized array block not exactly consumed")
            finally
                size >= 0 && (decoder.stop = outerstop)
            end
        end
    finally
        leave!(decoder)
    end
    return values
end

function syntheticvalue!(plan::MapTarget, decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    enter!(decoder)
    values = countwriter!(counter)
    try
        while true
            count, size = readblockcount(decoder)
            count == 0 && break
            outerstop = decoder.stop
            blocknext = -1
            if size >= 0
                blocknext = checked_add(decoder.pos, size)
                stop = blocknext - 1
                stop <= decoder.stop || dataerror(decoder, "sized map block exceeds the remaining bytes")
                valuevalues = active || !counter.countwriter ? 0 :
                    fastskipvalues(plan.values, decoder.budget.limits)
                if !active && (!counter.countwriter || valuevalues >= 0)
                    skipped = countwriterproduct!(counter, count, valuevalues)
                    values = syntheticvalueadd(counter, values, skipped)
                    decoder.pos = blocknext
                    continue
                end
                decoder.stop = stop
            end
            try
                for _ in 1:count
                    skiplen(decoder)
                    values = syntheticvalueadd(counter, values,
                                               syntheticvalue!(plan.values, decoder,
                                                               active, counter))
                end
                size >= 0 && decoder.pos != blocknext &&
                    dataerror(decoder, "sized map block not exactly consumed")
            finally
                size >= 0 && (decoder.stop = outerstop)
            end
        end
    finally
        leave!(decoder)
    end
    return values
end

function syntheticvalue!(plan::RecordTarget, decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    enter!(decoder)
    values = countwriter!(counter)
    try
        for field in plan.plans
            values = syntheticvalueadd(counter, values,
                                       syntheticvalue!(field, decoder, active,
                                                       counter))
        end
    finally
        leave!(decoder)
    end
    return values
end

function syntheticvalue!(plan::ResolvedNullTarget, decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    return syntheticvalue!(plan.plan, decoder, active, counter)
end

function syntheticvalue!(plan::ResolvedNullableTarget, decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    isempty(plan.branches) && dataerror(decoder, "empty union has no datum")
    values = countwriter!(counter)
    index = readindex(decoder, length(plan.branches))
    return syntheticvalueadd(counter, values,
                             syntheticvalue!(plan.branches[index], decoder,
                                             active, counter))
end

function syntheticvalue!(plan::ResolvedRecordTarget, decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    enter!(decoder)
    values = countwriter!(counter)
    try
        for field in plan.plans
            values = syntheticvalueadd(counter, values,
                                       syntheticvalue!(field, decoder, active,
                                                       counter))
        end
        if active
            for index in eachindex(plan.defaults)
                isassigned(plan.defaults, index) || continue
                default = plan.defaults[index]
                default isa ResolvedReaderDefault || continue
                checkdefaultdepth(default.plan, decoder)
                countsynthetic!(counter, default.plan.values)
                values = syntheticvalueadd(counter, values,
                                           default.plan.values)
            end
        end
    finally
        leave!(decoder)
    end
    return values
end

"""
    decodetyped(plan::TypedPlan, d::Decoder, names) -> value

Decode one value of `plan` (counted like the generic `decode`); `names` is the admission object.
"""
@inline function decodetyped(p::TypedPlan, d::Decoder, names)
    countvalues!(d.budget)
    return typedvalue(p, d, names)
end

function typedvalue(p::GenericTarget, d::Decoder, names)
    return decodevalue(p.plan, d)
end
# Reached only through a recursive reference whose record fell back: converts where it occurs.
function typedvalue(p::SemanticTarget{T}, d::Decoder, names) where {T}
    return semanticvalue(T, decodevalue(p.plan, d), names, p.schema, d.budget)
end

function typedvalue(p::LeafTarget{T}, d::Decoder, names) where {T}
    return convertleaf(T, decodevalue(p.plan, d), d.budget)::T
end

function typedvalue(p::LeafTarget{String,EnumPlan}, d::Decoder, names)
    return p.plan.schema.symbols[readindex(d, length(p.plan.schema.symbols))]
end

function typedvalue(p::SymbolTarget{StringPlan}, d::Decoder, names)
    value = readstring(d)
    symbol = admit!(names, value; budget=d.budget)
    bytes = sizeof(value)
    value = nothing
    release!(d.budget, stringbytes(bytes))
    return symbol
end

function typedvalue(p::RefTarget{T}, d::Decoder, names) where {T}
    return typedvalue(p.plan::TypedPlan, d, names)::T
end

function typedvalue(p::SymbolTarget{EnumPlan}, d::Decoder, names)
    syms = p.plan.schema.symbols
    return admit!(names, syms[readindex(d, length(syms))]; budget=d.budget)
end

function typedvalue(p::EnumTarget{T}, d::Decoder, names) where {T}
    syms = p.plan.schema.symbols
    i = readindex(d, length(syms))
    m = p.members[i]
    m === nothing && throw(ConversionError(typediagnostic(
        d.budget, "target enum ", T, " has no member for ", boundedquoted(syms[i]))))
    return m
end

function typedvalue(p::EnumRemapTarget{String}, d::Decoder, names)
    return p.plan.reader.symbols[enumremapindex(p.plan, d)]
end

function typedvalue(p::EnumRemapTarget{Symbol}, d::Decoder, names)
    sym = p.plan.reader.symbols[enumremapindex(p.plan, d)]
    return admit!(names, sym; budget=d.budget)
end

function typedvalue(p::EnumRemapNativeTarget{T}, d::Decoder, names) where {T}
    i = enumremapindex(p.plan, d)
    m = p.members[i]
    m === nothing && throw(ConversionError(typediagnostic(
        d.budget, "target enum ", T, " has no member for ",
        boundedquoted(p.plan.reader.symbols[i]))))
    return m
end

function typedvalue(p::NullableTarget{N}, d::Decoder, names) where {N}
    i = readindex(d, 2)
    if i == p.nullpos
        countvalues!(d.budget)
        N === Union{} && throw(ConversionError("null is not accepted by the target type"))
        return N === Missing ? missing : nothing
    end
    return decodetyped(p.inner, d, names)
end

function typedvalue(p::UnionTarget{T}, d::Decoder, names) where {T}
    n = length(p.branches)
    n == 0 && dataerror(d, "empty union has no datum")
    i = readindex(d, n)
    return decodetyped(p.branches[i], d, names)::T
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
            filltyped!(g, p.items, p.inlineshell, d, names, count, stop)
            d.pos == stop + 1 || dataerror(d, "sized array block not exactly consumed")
        else
            filltyped!(g, p.items, p.inlineshell, d, names, count, -1)
        end
    end
    leave!(d)
    if g === nothing
        reserve!(d.budget, STORAGE[].vector)
        out = Vector{E}(undef, 0)
        allocated!(d.budget, STORAGE[].vector)
        return out
    end
    return finish!(g, d)
end

function filltyped!(g::GrowBuf{E}, items::TypedPlan, inlineshell::Int, d::Decoder, names,
                    count::Int, stop::Int) where {E}
    outerstop = d.stop
    stop >= 0 && (stop <= d.stop || dataerror(d, "sized block exceeds the remaining bytes"); d.stop = stop)
    for _ in 1:count
        push!(g, d, decodetyped(items, d, names))
        release!(d.budget, inlineshell)                 # the concrete immutable shell now lives in its vector slot
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
        checkcount(d, count, satadd(p.minsize, 1))
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
            release!(d.budget, p.inlineshell)          # the concrete immutable shell now lives in its vector slot
        end
        size >= 0 && (d.pos == d.stop + 1 || dataerror(d, "sized map block not exactly consumed"))
        d.stop = outerstop
    end
    leave!(d)
    keys = finish!(ks, d)
    vals = if vs === nothing
        reserve!(d.budget, STORAGE[].vector)
        empty = Vector{V}(undef, 0)
        allocated!(d.budget, STORAGE[].vector)
        empty
    else
        finish!(vs, d)
    end
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
    allocated!(d.budget, p.shell)                      # the value's shell (inline shells release at their vector slot)
    leave!(d)
    return v
end

"""
The heap shell of one decoded `T`, measured once per plan from an empty probe (`Expr(:new, T)` with no
fields: reference fields stay undefined, so `Base.summarysize` reports exactly the header plus the
inline layout — including nested inline structs — and no referenced payload; plan §4.4, R10). The
checked fallback bound covers types `:new` cannot probe.
"""
@generated function emptyprobe(::Type{T}) where {T}
    return Expr(:new, T)
end

"A field type whose instances live inline in their parent and charge their own target shell."
function inlinestruct(::Type{F}) where {F}
    return isconcretetype(F) && isstructtype(F) && !ismutabletype(F) && !isbitstype(F) &&
           !(F <: AbstractArray) && !(F <: AbstractString) && !(F <: AbstractDict)
end

"Measure the complete immutable shell that becomes part of a concrete vector element slot."
function measuredinlineshell(::Type{T}, budget::Budget) where {T}
    bound = checked_add(checked_add(sizeof(T), checked_mul(8, fieldcount(T))), 64)
    reserve!(budget, bound)                            # the transient probe object, built next
    settled = false
    try
        probe = try
            emptyprobe(T)
        catch
            allocated!(budget, bound)
            settled = true
            return bound
        end
        allocated!(budget, bound)
        settled = true
        return max(Int(Base.summarysize(probe)), 8)
    finally
        settled ? release!(budget, bound) : unreserve!(budget, bound)
    end
end

function measuredshell(::Type{T}, budget::Union{Nothing,Budget}=nothing) where {T}
    isbitstype(T) && return 0
    bound = checked_add(checked_add(sizeof(T), checked_mul(8, fieldcount(T))), 64)
    budget === nothing || reserve!(budget, bound)      # the transient probe object, built next
    settled = false
    try
        shell = if ismutabletype(T) || isstructtype(T)
            probe = try
                emptyprobe(T)
            catch
                budget === nothing || settled || (allocated!(budget, bound); settled = true)
                return bound
            end
            budget === nothing || settled || (allocated!(budget, bound); settled = true)
            marginal = Int(Base.summarysize(probe))
            for i in 1:fieldcount(T)                   # inline nested structs charge their own shells
                F = fieldtype(T, i)
                if inlinestruct(F) && VERSION >= v"1.11"
                    marginal -= measuredshell(F, budget)
                end
            end
            max(marginal, 8)
        else
            16 + sizeof(T)
        end
        return shell
    finally
        if budget !== nothing
            settled ? release!(budget, bound) : unreserve!(budget, bound)
        end
    end
end

function shellbytes(::Type{T}) where {T}
    return isbitstype(T) ? 0 : 16 + sizeof(T)
end

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
    args = []
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

function convertleaf(::Type{T}, value) where {T}
    return convertleaf(T, value, nothing)
end

function convertleaf(::Type{Nothing}, ::Missing, budget)
    return nothing
end

function convertleaf(::Type{Float64}, v::Float32, budget)
    return Float64(v)
end

function convertleaf(::Type{Float16}, v::Float32, budget)
    return Float16(v)
end

function convertleaf(::Type{DateTime}, v::Union{Timestamp,LocalTimestamp}, budget)
    return DateTime(v)
end

function convertleaf(::Type{Vector{UInt8}}, v::Fixed, budget)
    bytes = v.bytes
    v = nothing
    budget === nothing || release!(budget, STORAGE[].fixed)
    return bytes
end

function convertleaf(::Type{T}, v::Union{Int32,Int64}, budget) where {T<:Integer}
    fits = T <: Signed ? (typemin(T) <= Int64(v) <= typemax(T)) : (v >= 0 && UInt64(v) <= typemax(T))
    fits || throw(ConversionError(typediagnostic(
        budget, "target integer type ", T, " cannot represent ", Int64(v))))
    return T(v)
end

function convertleaf(::Type{Char}, s::String, budget)
    n = ncodeunits(s)
    n > 0 && n == ncodeunits(s[1]) ||
        throw(ConversionError(diagnosticstring(
            budget, "expected a one-character string for Char, got ",
            boundedquoted(s))))
    value = s[1]
    s = nothing
    budget === nothing || release!(budget, stringbytes(n))
    return value
end

function convertleaf(::Type{T}, v::Fixed, budget) where {T<:Tuple}
    value = ntuple(i -> @inbounds(v.bytes[i]), Val(length(T.parameters)))
    bytes = length(v.bytes)
    v = nothing
    budget === nothing || release!(budget, fixedbytes(bytes))
    return value
end

# ---- semantic route ----------------------------------------------------------------------------------

const ADMISSION_KEY = :avro_symbol_admission

function withadmission(f, names)
    return task_local_storage(f, ADMISSION_KEY, names)
end

function currentadmission()
    return get(task_local_storage(), ADMISSION_KEY, DEFAULT_ADMISSION)
end

"""
    semanticvalue(T, generic, names, schema)

Convert a generic value to `T` through `StructUtils.make` under `AvroStyle`, in caller space; conversion
failures are `ConversionError`s (limit errors propagate).
"""
function semanticvalue(::Type{T}, v, names, schema::Schema,
                       budget::Union{Nothing,Budget}=nothing) where {T}
    return withadmission(names) do
        try
            StructUtils.make(T, v, AvroStyle(schema))
        catch e
            e isa Union{ArgumentError,MethodError,InexactError,TypeError,KeyError,BoundsError,DomainError} || rethrow()
            structutilshookexception(catch_backtrace()) && rethrow()
            throw(ConversionError(typediagnostic(
                budget, "cannot convert the decoded value to ", T,
                "; StructUtils raised ", typeof(e))))
        end
    end
end

"Whether a caught conversion error passed through a caller-defined StructUtils make/lift method."
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

function finishtyped(p::SemanticTarget{T}, v, names) where {T}
    return semanticvalue(T, v, names, p.schema)
end

function finishtyped(p, v, names)
    return v
end

# StructUtils integration: generic values as schema-aware sources. A semantic conversion owns one
# mutable schema stack. StructUtils calls are synchronous, so each child schema can be installed around
# the recursive callback without wrapping or changing the raw value seen by user hooks.
function currentsemanticschema(st::AvroStyle)
    stack = st.schemas
    (stack === nothing || isempty(stack)) && return nothing
    return stack[end]
end

function withsemanticschema(f, st::AvroStyle, schema::Schema)
    stack = st.schemas
    stack === nothing && return f()
    push!(stack, schema)
    try
        return f()
    finally
        pop!(stack)
    end
end

function semanticnode(schema::UnionSchema, value)
    if value isa UnionValue
        1 <= value.index <= length(schema.branches) ||
            throw(ArgumentError("union branch index $(value.index) out of range (1:$(length(schema.branches)))"))
        return (schema.branches[value.index], value.value)
    end
    nullable = nullablebranch(schema)
    nullable == 0 && return (schema, value)
    branch = value === missing || value === nothing ? nullable : 3 - nullable
    return (schema.branches[branch], value)
end

function semanticnode(schema::Schema, value)
    return (schema, value)
end

function semanticnode(st::AvroStyle, value)
    schema = currentsemanticschema(st)
    schema === nothing && return (nothing, value)
    return semanticnode(schema, value)
end

function StructUtils.applyeach(st::AvroStyle, f, r::Record)
    return applyeachrecord(st, f, r)
end

function StructUtils.applyeach(st::AvroStyle, f::StructUtils.StructStyle, r::Record)
    return applyeachrecord(st, f, r)   # disambiguates the (f, style, x) form
end

function applyeachrecord(st::AvroStyle, f, r::Record)
    s = getfield(r, :schema)
    vals = getfield(r, :values)
    for (i, fld) in enumerate(s.fields)
        ret = withsemanticschema(st, fld.schema) do
            return f(fld.name, vals[i])
        end
        ret isa StructUtils.EarlyReturn && return ret
    end
    return StructUtils.defaultstate(st)
end

function StructUtils.applyeach(st::AvroStyle, f, values::AbstractArray)
    return applyeacharray(st, f, values)
end

function StructUtils.applyeach(st::AvroStyle, f::StructUtils.StructStyle,
                               values::AbstractArray)
    return applyeacharray(st, f, values)
end

function StructUtils.applyeach(st::AvroStyle, f, values::JSON.LazyArray)
    return applyeacharray(st, f, values)
end

function StructUtils.applyeach(st::AvroStyle, f::StructUtils.StructStyle,
                               values::JSON.LazyArray)
    return applyeacharray(st, f, values)
end

function StructUtils.applyeach(st::AvroStyle, f, values::StructUtils.Selectors.List)
    return applyeacharray(st, f, values)
end

function StructUtils.applyeach(st::AvroStyle, f::StructUtils.StructStyle,
                               values::StructUtils.Selectors.List)
    return applyeacharray(st, f, values)
end

function applyeacharray(st::AvroStyle, f, values::AbstractArray)
    schema, source = semanticnode(st, values)
    items = schema isa ArraySchema ? schema.items : nothing
    for i in eachindex(source)
        value = @inbounds isassigned(source, i) ? source[i] : nothing
        ret = if items === nothing
            f(StructUtils.lowerkey(st, i), StructUtils.lower(st, value))
        else
            withsemanticschema(st, items) do
                return f(StructUtils.lowerkey(st, i), value)
            end
        end
        ret isa StructUtils.EarlyReturn && return ret
    end
    return StructUtils.defaultstate(st)
end

function StructUtils.applyeach(st::AvroStyle, f, pairs::AbstractVector{Pair{K,V}}) where {K,V}
    return applyeachpairs(st, f, pairs)
end

function StructUtils.applyeach(st::AvroStyle, f::StructUtils.StructStyle,
                               pairs::AbstractVector{Pair{K,V}}) where {K,V}
    return applyeachpairs(st, f, pairs)
end

function StructUtils.applyeach(st::AvroStyle, f,
                               pairs::StructUtils.Selectors.List{Pair{K,V}}) where {K,V}
    return applyeachpairs(st, f, pairs)
end

function StructUtils.applyeach(st::AvroStyle, f::StructUtils.StructStyle,
                               pairs::StructUtils.Selectors.List{Pair{K,V}}) where {K,V}
    return applyeachpairs(st, f, pairs)
end

function applyeachpairs(st::AvroStyle, f, pairs::AbstractVector{Pair{K,V}}) where {K,V}
    for (key, value) in pairs
        ret = f(StructUtils.lowerkey(st, key), StructUtils.lower(st, value))
        ret isa StructUtils.EarlyReturn && return ret
    end
    return StructUtils.defaultstate(st)
end

function StructUtils.applyeach(st::AvroStyle, f, values::Map)
    return applyeachmap(st, f, values)
end

function StructUtils.applyeach(st::AvroStyle, f::StructUtils.StructStyle, values::Map)
    return applyeachmap(st, f, values)
end

function applyeachmap(st::AvroStyle, f, values::Map)
    schema, source = semanticnode(st, values)
    items = schema isa MapSchema ? schema.values : nothing
    for (key, value) in source
        ret = if items === nothing
            f(StructUtils.lowerkey(st, key), StructUtils.lower(st, value))
        else
            withsemanticschema(st, items) do
                return f(StructUtils.lowerkey(st, key), value)
            end
        end
        ret isa StructUtils.EarlyReturn && return ret
    end
    return StructUtils.defaultstate(st)
end

# StructUtils lowers non-struct-like sources at the root only: the identity-bearing generic values are
# representations, not structs to traverse.
function StructUtils.structlike(::AvroStyle, ::Type{<:Union{Fixed,EnumValue,UnionValue}})
    return false
end

function StructUtils.lower(::AvroStyle, x::EnumValue)
    return String(x)
end

function StructUtils.lower(::AvroStyle, x::Fixed)
    return x.bytes
end

function StructUtils.lower(::AvroStyle, x::UnionValue)
    return x.value
end

function StructUtils.lift(st::AvroStyle, ::Type{T}, x::UnionValue) where {T}
    schema = currentsemanticschema(st)
    schema isa UnionSchema || return StructUtils.lift(st, T, x.value)
    branch, value = semanticnode(schema, x)
    member = semanticmember(T, branch, value, st)
    member === nothing && return StructUtils.lift(st, T, value)
    return withsemanticschema(st, branch) do
        return StructUtils.make(st, member, value)
    end
end

function semanticmember(::Type{T}, schema::Schema, value, st::AvroStyle) where {T}
    T isa Union || return T
    members = Base.uniontypes(T)
    for member in members
        value isa member && return member
    end
    for member in members
        semanticcompatible(member, schema, st) && return member
    end
    hooked = nothing
    for member in members
        customhooks(member) || continue
        hooked === nothing || return nothing
        hooked = member
    end
    return hooked
end

function semanticcompatible(::Type{T}, schema::Schema, st::AvroStyle) where {T}
    T === juliatype(schema) && return true
    if schema isa UnionSchema
        members = T isa Union ? Base.uniontypes(T) : (T,)
        for branch in schema.branches
            any(member -> semanticcompatible(member, branch, st), members) || return false
        end
        return true
    end
    schema isa NullSchema && return T === Missing || T === Nothing
    schema isa BooleanSchema && return T === Bool
    if schema isa Union{IntSchema,LongSchema}
        schema.logical === nothing && return T <: Integer && T !== Bool
        schema.logical isa Union{TimestampMillis,TimestampMicros,TimestampNanos,
                                 LocalTimestampMillis,LocalTimestampMicros,LocalTimestampNanos} &&
            return T === DateTime
        return false
    end
    schema isa Union{FloatSchema,DoubleSchema} && return T <: AbstractFloat
    if schema isa BytesSchema
        schema.logical isa DecimalLogical && return T <: DataDecimals.AbstractDecimal || T === WideDecimal
        return T <: AbstractVector{UInt8}
    end
    if schema isa StringSchema
        schema.logical isa UUIDLogical && T === UUID && return true
        return T <: AbstractString || T === Symbol || T === Char || T === UUID
    end
    if schema isa FixedSchema
        schema.logical isa DecimalLogical && return T <: DataDecimals.AbstractDecimal || T === WideDecimal
        schema.logical isa UUIDLogical && return T === UUID
        schema.logical isa DurationLogical && return T === Duration || T === Durations.Duration
        return T === Fixed || T <: AbstractVector{UInt8} || isbytetuple(T, schema.size)
    end
    schema isa EnumSchema && return T === EnumValue || T <: Base.Enum || T <: AbstractString || T === Symbol
    if schema isa ArraySchema
        StructUtils.arraylike(st, T) || return false
        return hasmethod(eltype, Tuple{Type{T}}) ? semanticcompatible(eltype(T), schema.items, st) : true
    end
    if schema isa MapSchema
        StructUtils.dictlike(st, T) || return false
        return hasmethod(valtype, Tuple{Type{T}}) ? semanticcompatible(valtype(T), schema.values, st) : true
    end
    if schema isa RecordSchema
        return isconcretetype(T) &&
               (T <: NamedTuple || (StructUtils.structlike(st, T) &&
                                     !StructUtils.arraylike(st, T) && !StructUtils.dictlike(st, T)))
    end
    return false
end

function StructUtils.nulllike(st::AvroStyle, x::UnionValue)
    schema = currentsemanticschema(st)
    if schema isa UnionSchema && 1 <= x.index <= length(schema.branches)
        return schema.branches[x.index] isa NullSchema
    end
    return StructUtils.nulllike(x.value)
end

function StructUtils.lift(st::AvroStyle, ::Type{T}, x::EnumValue) where {T}
    return StructUtils.lift(st, T, String(x))
end

function StructUtils.lift(st::AvroStyle, ::Type{T}, x::Fixed) where {T}
    return StructUtils.lift(st, T, x.bytes)
end
# zero-dimensional array targets (StructUtils' own special case) unwrap the same way
function StructUtils.lift(st::AvroStyle, ::Type{A}, x::UnionValue) where {A<:AbstractArray{T,0}} where {T}
    return StructUtils.lift(st, A, x.value)
end

function StructUtils.lift(st::AvroStyle, ::Type{A}, x::EnumValue) where {A<:AbstractArray{T,0}} where {T}
    return StructUtils.lift(st, A, String(x))
end

function StructUtils.lift(st::AvroStyle, ::Type{A}, x::Fixed) where {A<:AbstractArray{T,0}} where {T}
    return StructUtils.lift(st, A, x.bytes)
end

function StructUtils.lift(::AvroStyle, ::Type{Symbol}, x::AbstractString)
    return (admit!(currentadmission(), x), nothing)
end

function StructUtils.liftkey(::AvroStyle, ::Type{Symbol}, x::AbstractString)
    return admit!(currentadmission(), x)
end

function StructUtils.lift(::Type{DateTime}, x::Union{Timestamp,LocalTimestamp})
    return DateTime(x)
end

function StructUtils.lift(::Type{T}, x::Union{Timestamp,LocalTimestamp}) where {T<:Union{Timestamp,LocalTimestamp}}
    return T(x.ticks)
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
    return _decimalinput(x), StructUtils.defaultstate(st)
end
