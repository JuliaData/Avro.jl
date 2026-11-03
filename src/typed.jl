# Typed decoding: a typed node decodes one schema node into a caller-supplied Julia type `T`. Node types are
# parametric only on target types, so a typed reader's type follows from `T` alone and its code compiles
# once per target type, whatever the schema; nodes reach their plans through closed `isa` chains. Plain
# data types are built straight from their decoded fields through their positional constructors; other
# targets decode generically and convert through `StructUtils.make(T, generic, AvroStyle())`, which runs
# through run-time dispatch (`runtimecall`) and so is left out of `juliac --trim` programs.

abstract type TypedNode{T} end

# The generic value: `T` is `Any`, or `Record` for a record schema.
struct GenericNode{T} <: TypedNode{T}
    plan::Plan
end

# Decode generically, then convert through StructUtils (`AvroStyle`).
struct SemanticNode{T} <: TypedNode{T}
    plan::Plan
    schema::Schema
end

# A leaf whose generic value is a `T` or converts to one (`convertleaf`).
struct LeafNode{T} <: TypedNode{T}
    plan::Plan
end

# A `string` interned as a `Symbol`.
struct SymbolNode <: TypedNode{Symbol} end

# An enum by reader symbol position into `String`, `Symbol` or a `Base.Enum` (`nothing`: no member).
struct EnumNode{T} <: TypedNode{T}
    plan::Plan                                 # `EnumPlan`, or `EnumRemapPlan` under resolution
    schema::EnumSchema                         # the reader enum
    members::Vector{Union{Nothing,T}}
end

# An array into `Vector{E}`.
struct ArrayNode{T,E} <: TypedNode{T}
    items::TypedNode
    minsize::Int
end

# A map into `Dict{K,V}`, `K` `String` or `Symbol`.
struct MapNode{T,K,V} <: TypedNode{T}
    values::TypedNode
    minsize::Int
end

# A union whose branches are null or decode into `X`, the non-null member of `T` (or `T` itself, which then
# rejects a null with a `ConversionError`): a two-branch nullable union, or a writer union resolved into one
# branch by branch. `branches[i]` decodes branch `i`; `nothing` marks a null (`plans[i]` a `NullPlan`) or a
# writer branch the reader cannot resolve (an `UnresolvableBranch`).
struct NullableNode{T,X} <: TypedNode{T}
    plans::Vector{Plan}
    branches::Vector{Union{Nothing,TypedNode}}
end

# Any other union into a Julia type whose members decode the branches: branch `i` decodes into member
# `members[i]` of `Base.uniontypes(T)`.
struct UnionNode{T} <: TypedNode{T}
    branches::Vector{TypedNode}
    members::Vector{Int}
end

# A record decoded in wire order straight into a `NamedTuple` or plain struct. Wire step `j` fills target
# field `slots[j]` through `nodes[j]`, or is skipped through `plans[j]` when `slots[j] == 0`; a target field
# no step fills (`!filled[k]`) takes a fresh copy of `defaults[k]`, a value of the field's type. Mutable, so
# a recursive record refers to its own node while it is built.
mutable struct RecordNode{T} <: TypedNode{T}
    const plans::Vector{Plan}
    const nodes::Vector{Union{Nothing,TypedNode}}
    const slots::Vector{Int}
    const filled::Vector{Bool}
    const defaults::Vector{Any}
    aligned::Bool                              # step `k` fills field `k` for every field, and there are no others
end

# ---- construction -----------------------------------------------------------------------------------

# The name of `T` for messages, built when the method compiles.
@generated function typestring(::Type{T}) where {T}
    return string(T)
end

# The record nodes built so far by (read plan, target type); a reader record resolved against two writer
# records has two read plans. An entry stays complete for its key, including one built inside a union-member
# or record attempt that later fell back to the semantic route.
const TypedMemo = IdDict{Any,TypedNode}

"The typed node decoding `plan` (the generic or resolving plan of `reader`) into `T`."
Base.@nospecializeinfer function typednode(::Type{T}, @nospecialize(reader::Schema), @nospecialize(plan::Plan)) where {T}
    return buildnode(T, reader, plan, TypedMemo())
end

Base.@nospecializeinfer function buildnode(::Type{T}, @nospecialize(s::Schema), @nospecialize(p::Plan), memo::TypedMemo) where {T}
    T === Any && return GenericNode{Any}(p)
    if !(s isa Union{UnionSchema,ArraySchema,MapSchema}) && T === juliatype(s)
        return T === Record ? GenericNode{Record}(p) : LeafNode{T}(p)
    end
    customhooks(T) && return SemanticNode{T}(p, s)
    if isresolving(p)
        node = runtimecall(buildresolvednode, T, s, p, memo)     # compiled when a reader schema first needs it
        return node === nothing ? SemanticNode{T}(p, s) : node::TypedNode
    end
    s isa UnionSchema && return buildunionnode(T, s, p::UnionPlan, memo)
    if T isa Union
        N, X = splitoptional(T)
        s isa NullSchema && N !== Union{} && return LeafNode{N}(p)
        X === nothing && return SemanticNode{T}(p, s)
        return buildnode(X, s, p, memo)          # a non-null schema never produces the null member
    end
    recordlike(T) && s isa RecordSchema && return buildrecordnode(T, s, p, memo)
    T <: Vector && s isa ArraySchema && return buildarraynode(T, s, p::ArrayPlan, memo)
    T <: Dict && s isa MapSchema && return buildmapnode(T, s, p::MapPlan, memo)
    return buildleafnode(T, s, p)        # the semantic node when `T` cannot hold the schema's values directly
end

# `Union{Missing,X}` → `(Missing, X)`, `Union{Nothing,X}` → `(Nothing, X)`; `X === nothing` when `T` is not of
# that shape (several members, both null kinds, or not a union). Generated, so both are constants where
# `T` is.
@generated function splitoptional(::Type{T}) where {T}
    if Missing <: T
        X = Base.nonmissingtype(T)
        return (X === Union{} || X isa Union) ? :((Missing, nothing)) : :((Missing, $X))
    elseif Nothing <: T
        X = Base.nonnothingtype(T)
        return (X === Union{} || X isa Union) ? :((Nothing, nothing)) : :((Nothing, $X))
    end
    return :((Union{}, nothing))
end

# The schema node and plan a node is built for. Generated builders take it whole: a generated function
# specializes on every argument's run-time type, and this one is concrete.
struct NodeSite
    schema::Schema
    plan::Plan
    memo::TypedMemo
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
const FIELD_DEFAULTS_CACHE = HookCache(ReentrantLock(), UInt(0), IdDict{Any,Bool}())
const HOOK_CACHE_SIZE = 1024

# The answer of the method query `f(T)`. Queries run through `runtimecall`: trimmed programs, which leave
# method-table reflection out, answer `false`, so they never consult StructUtils hooks or overridden
# `propertynames`.
Base.@nospecializeinfer function cachedanswer(f::F, cache::HookCache, @nospecialize(T::Type)) where {F}
    world = Base.get_world_counter()
    Base.@lock cache.lock begin
        if cache.world != world
            empty!(cache.answers)
            cache.world = world
        end
        answer = get(cache.answers, T, nothing)
        answer === nothing || return answer::Bool
    end
    answer = try                                       # outside the lock: the query may compile
        runtimecall(f, T)::Bool
    catch err
        notcompiled(err, f) || rethrow()
        false
    end
    Base.@lock cache.lock begin
        if cache.world == world                        # unless a newer world emptied the cache meanwhile
            length(cache.answers) >= HOOK_CACHE_SIZE && empty!(cache.answers)
            cache.answers[T] = answer
        end
    end
    return answer
end

# Whether a `StructUtils.make` or `StructUtils.lift` method outside StructUtils and Avro applies to `T`
# under `AvroStyle` (such targets always take the semantic route).
Base.@nospecializeinfer function customhooks(@nospecialize(T::Type))
    return cachedanswer(methodhooks, HOOK_CACHE, T)
end

# Whether `T` overrides `propertynames` (a DataFrame row, say), so that its properties are its columns.
Base.@nospecializeinfer function usesproperties(@nospecialize(T::Type))
    T <: NamedTuple && return false
    return cachedanswer(overridespropertynames, PROPERTY_CACHE, T)
end

function overridespropertynames(@nospecialize(T::Type))
    return which(propertynames, Tuple{T}).module !== Base
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

# Default factories run per value on the semantic route, including defaults that depend on other fields.
Base.@nospecializeinfer function customfielddefaults(@nospecialize(T::Type))
    return cachedanswer(methodfielddefaults, FIELD_DEFAULTS_CACHE, T)
end

function methodfielddefaults(@nospecialize(T::Type))
    for signature in (Tuple{AvroStyle,Type{T}}, Tuple{AvroStyle,Type{T},Any})
        for method in methods(StructUtils.fielddefaults, signature)
            method.module in HOOK_MODULES || return true
        end
    end
    return false
end

function buildunionnode(::Type{T}, s::UnionSchema, p::UnionPlan, memo::TypedMemo) where {T}
    nb = nullablebranch(s)
    node = nb == 0 ? branchnodes(T, s, p, memo) : nullablenode(T, s, p, nb, memo)
    return node === nothing ? SemanticNode{T}(p, s) : node
end

function nullablenode(::Type{T}, s::UnionSchema, p::UnionPlan, nb::Int, memo::TypedMemo) where {T}
    X = T isa Union ? splitoptional(T)[2] : T
    X === nothing && return nothing
    inner = buildnode(X, s.branches[3 - nb], p.branches[3 - nb], memo)
    inner isa SemanticNode && return nothing
    branches = Union{Nothing,TypedNode}[inner, inner]
    branches[nb] = nothing
    return NullableNode{T,X}(copy(p.branches), branches)
end

function branchnodes(::Type{T}, s::UnionSchema, p::UnionPlan, memo::TypedMemo) where {T}
    branches = TypedNode[]
    members = Int[]
    for (b, bp) in zip(s.branches, p.branches)
        found = membernode(T, NodeSite(b, bp, memo))
        found === nothing && return nothing
        push!(branches, found.first)
        push!(members, found.second)
    end
    return UnionNode{T}(branches, members)
end

# The node of the first member of `T` that decodes the site's union branch directly, and that member's
# position in `Base.uniontypes(T)`: the branch's own Julia type first, then the others in order.
@generated function membernode(::Type{T}, site::NodeSite) where {T}
    members = T isa Union ? Base.uniontypes(T) : Any[T]
    body = Expr(:block, :(natural = juliatype(site.schema)))
    for pass in (:own, :others), (k, M) in enumerate(members)
        test = pass === :own ? :(natural === $M) : :(natural !== $M)
        push!(body.args, :($test && (node = buildnode($M, site.schema, site.plan, site.memo);
                                     isdirect(node) && return Pair{TypedNode,Int}(node, $k))))
    end
    push!(body.args, :(return nothing))
    return body
end

# Whether a node decodes with no StructUtils conversion anywhere below it.
Base.@nospecializeinfer function isdirect(@nospecialize(node::TypedNode), seen::Base.IdSet{Any}=Base.IdSet{Any}())
    node isa SemanticNode && return false
    node in seen && return true
    push!(seen, node)
    node isa ArrayNode && return isdirect(node.items, seen)
    node isa MapNode && return isdirect(node.values, seen)
    node isa UnionNode && return all(c -> isdirect(c, seen), node.branches)
    children = node isa RecordNode ? node.nodes : node isa NullableNode ? node.branches : nothing
    children === nothing && return true
    for c in children
        c === nothing || isdirect(c, seen) || return false
    end
    return true
end

Base.@nospecializeinfer function buildleafnode(::Type{T}, @nospecialize(s::Schema), @nospecialize(p::Plan)) where {T}
    T === Symbol && p isa StringPlan && return SymbolNode()
    isenumtarget(T) && p isa EnumPlan && s isa EnumSchema && return enumnode(T, s, p)
    leaftarget(T) && leafcompatible(T, p) && return LeafNode{T}(p)
    return SemanticNode{T}(p, s)
end

function isenumtarget(@nospecialize(T))
    return T === String || T === Symbol || (T isa DataType && T <: Base.Enum)
end

Base.@nospecializeinfer function enumnode(::Type{T}, s::EnumSchema, @nospecialize(p::Plan)) where {T}
    return EnumNode{T}(p, s, enummembers(T, s))
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

# Each leaf plan and the type of its generic value (a decimal plan's is one of two).
const LEAF_PLAN_VALUES = (
    (LongPlan, Int64), (StringPlan, String), (DoublePlan, Float64), (IntPlan, Int32), (BoolPlan, Bool),
    (FloatPlan, Float32), (BytesPlan, Vector{UInt8}), (NullPlan, Missing), (EnumPlan, EnumValue),
    (FixedPlan, Fixed), (DatePlan, Date), (TimeMillisPlan, Time), (TimeMicrosPlan, Time),
    (TimestampPlan{Millisecond}, Timestamp{Millisecond}), (TimestampPlan{Microsecond}, Timestamp{Microsecond}),
    (TimestampPlan{Nanosecond}, Timestamp{Nanosecond}), (LocalTimestampPlan{Millisecond}, LocalTimestamp{Millisecond}),
    (LocalTimestampPlan{Microsecond}, LocalTimestamp{Microsecond}), (LocalTimestampPlan{Nanosecond}, LocalTimestamp{Nanosecond}),
    (DecimalPlan, Decimal), (DecimalPlan, WideDecimal), (UUIDStringPlan, UUID), (UUIDFixedPlan, UUID), (DurationPlan, Duration),
)

# The reader leaf types a promotion produces (`promotedvalue`).
const PROMOTION_TARGETS = (Int64, Float32, Float64, Time, String, UUID, Vector{UInt8}, Decimal, WideDecimal,
                           Timestamp{Millisecond}, Timestamp{Microsecond}, Timestamp{Nanosecond},
                           LocalTimestamp{Millisecond}, LocalTimestamp{Microsecond}, LocalTimestamp{Nanosecond})

# Whether `convertleaf` converts the generic value of plans of type `P` to `T` (a type other than that
# value's own); a fixed's size is checked separately.
Base.@nospecializeinfer function leafconverts(@nospecialize(T), @nospecialize(P))
    T isa DataType || return false
    P === NullPlan && return T === Nothing
    (P === IntPlan || P === LongPlan) && return T <: Integer && T !== Bool && isbitstype(T)
    P === FloatPlan && return T === Float64 || T === Float16
    P === StringPlan && return T === Char
    P === FixedPlan && return T === Vector{UInt8} || (T <: Tuple && all(t -> t === UInt8, T.parameters))
    (P <: TimestampPlan || P <: LocalTimestampPlan) && return T === DateTime
    return false
end

Base.@nospecializeinfer function leafcompatible(::Type{T}, @nospecialize(p::Plan)) where {T}
    leafconverts(T, typeof(p)) || return false
    return !(p isa FixedPlan) || T === Vector{UInt8} || isbytetuple(T, p.schema.size)
end

function isbytetuple(::Type{T}, n::Int) where {T}
    return T <: Tuple && length(T.parameters) == n && all(t -> t === UInt8, T.parameters)
end

function buildarraynode(::Type{T}, s::ArraySchema, p::ArrayPlan, memo::TypedMemo) where {T}
    T <: Vector || return SemanticNode{T}(p, s)
    items = buildnode(eltype(T), s.items, p.items, memo)
    items isa SemanticNode && return SemanticNode{T}(p, s)
    return ArrayNode{T,eltype(T)}(items, p.minsize)
end

function buildmapnode(::Type{T}, s::MapSchema, p::MapPlan, memo::TypedMemo) where {T}
    (isconcretetype(T) && T <: Dict && keytype(T) in (String, Symbol)) || return SemanticNode{T}(p, s)
    values = buildnode(valtype(T), s.values, p.values, memo)
    values isa SemanticNode && return SemanticNode{T}(p, s)
    return MapNode{T,keytype(T),valtype(T)}(values, p.minsize)
end

# Plain and resolved records share the memo and the recursion placeholder; `recordnode!` fills the node.
Base.@nospecializeinfer function buildrecordnode(::Type{T}, s::RecordSchema, @nospecialize(p::Plan), memo::TypedMemo) where {T}
    cached = get(memo, (p, T), nothing)
    cached === nothing || return cached
    fastroute(T) || return (memo[(p, T)] = SemanticNode{T}(p, s))
    n = fieldcount(T)
    node = RecordNode{T}(Plan[], Union{Nothing,TypedNode}[], Int[], fill(false, n), Vector{Any}(undef, n), false)
    memo[(p, T)] = node                                # recursive references resolve to this node
    recordnode!(node, s, p, memo) && return node
    return (memo[(p, T)] = SemanticNode{T}(p, s))      # recursive references through `node` decode on its route
end

function recordlike(@nospecialize(T))
    return T <: NamedTuple || (isstructtype(T) && !(T <: NOT_RECORDLIKE))
end

# Eligibility for building `T` straight from decoded fields through its positional constructor: a concrete
# `NamedTuple` or struct that has one, no custom StructUtils hooks under `AvroStyle`, and no field tags other
# than `name` / `avro=(name, default)`.
function fastroute(::Type{T}) where {T}
    (isconcretetype(T) && recordlike(T)) || return false
    (T <: NamedTuple || hasmethod(T, Tuple{fieldtypes(T)...})) || return false
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

# Promotions, enum remaps and resolved records decode straight into `T`; a writer resolved into a
# two-branch nullable reader union goes through its non-null node, a null node or a per-writer-branch
# node. `nothing` when the semantic route must convert.
Base.@nospecializeinfer function buildresolvednode(::Type{T}, @nospecialize(s::Schema), @nospecialize(p::Plan), memo::TypedMemo) where {T}
    p isa PromotePlan && return buildleafnode(T, s, p)
    p isa EnumRemapPlan && return isenumtarget(T) && s isa EnumSchema ? enumnode(T, s, p) : nothing
    recordlike(T) && p isa ResolvedRecordPlan && s isa RecordSchema && return buildrecordnode(T, s, p, memo)
    (s isa UnionSchema && nullablebranch(s) != 0) || return nothing
    N, X = T isa Union ? splitoptional(T) : (Union{}, T)
    X === nothing && return nothing
    if p isa WrapPlan
        p.readerindex == nullablebranch(s) || return buildnode(X, s.branches[p.readerindex], p.inner, memo)
        return N === Union{} || !(p.inner isa NullPlan) ? nothing : LeafNode{N}(p.inner)
    end
    p isa UnionResolvePlan && N !== Union{} && return resolvednullable(T, X, s, p, memo)
    return nothing
end

function resolvednullable(::Type{T}, ::Type{X}, s::UnionSchema, p::UnionResolvePlan, memo::TypedMemo) where {T,X}
    nb = nullablebranch(s)
    inner = Union{Nothing,TypedNode}[]
    for (i, bp) in enumerate(p.branches)
        j = p.readerindex[i]
        if j == nb || bp isa UnresolvableBranch
            bp isa Union{NullPlan,UnresolvableBranch} || return nothing
            push!(inner, nothing)
            continue
        end
        node = buildnode(X, s.branches[j], bp, memo)
        node isa SemanticNode && return nothing
        push!(inner, node)
    end
    return NullableNode{T,X}(copy(p.branches), inner)
end

# Fill `node` for record `s` and its plan; `false` when the record must take the semantic route.
Base.@nospecializeinfer function recordnode!(node::RecordNode{T}, s::RecordSchema, @nospecialize(p::Plan), memo::TypedMemo) where {T}
    fieldof = recordfieldpositions(s, T)
    steps = p isa ResolvedRecordPlan ? p.steps : Pair{Int,Plan}[Pair{Int,Plan}(i, f) for (i, f) in enumerate((p::RecordPlan).fields)]
    for (slot, sp) in steps
        k = slot == 0 ? 0 : fieldof[slot]
        push!(node.plans, sp)
        push!(node.slots, k)
        push!(node.nodes, k == 0 ? nothing : fieldnode(T, k, NodeSite(s.fields[slot].schema, sp, memo)))
        k == 0 || (node.filled[k] = true)
    end
    covered = copy(node.filled)
    if p isa ResolvedRecordPlan
        for (slot, dp) in p.defaults
            k = fieldof[slot]
            k == 0 && continue
            v = typeddefault(fieldtype(T, k), dp)
            v === nothing && return false
            node.defaults[k] = something(v)
            covered[k] = true
        end
    end
    implicitdefaults!(node.defaults, T, covered) || return false
    node.aligned = node.slots == 1:fieldcount(T)
    return true
end

# The node decoding into target field `k`, built for the field's constant type.
@generated function fieldnode(::Type{T}, k::Int, site::NodeSite) where {T}
    body = Expr(:block)
    for j in 1:fieldcount(T)
        push!(body.args, :(k == $j && return buildnode($(fieldtype(T, j)), site.schema, site.plan, site.memo)))
    end
    push!(body.args, :(throw(ArgumentError("internal error: no field $k in the target"))))
    return body
end


# A reader default as a value of field type `T`: its generic value, or that converted as a leaf would be;
# `nothing` sends the record to the semantic route, which converts the default itself.
Base.@nospecializeinfer function typeddefault(@nospecialize(T::Type), dp::DefaultPlan)
    v = dp.value
    v isa T && return Some{Any}(v)
    try
        x = runtimecall(convertleaf, T, v)
        return x isa T ? Some{Any}(x) : nothing
    catch e
        e isa Union{MethodError,ConversionError} || rethrow()
        return nothing
    end
end

# Fill each target field nothing else covers with `missing` or `nothing`; `false` sends a type with
# StructUtils defaults to the semantic route.
function implicitdefaults!(defaults::Vector{Any}, ::Type{T}, covered::Vector{Bool}) where {T}
    all(covered) && return true
    customfielddefaults(T) && return false
    for k in 1:fieldcount(T)
        covered[k] && continue
        ft = fieldtype(T, k)
        (Missing <: ft || Nothing <: ft) || throw(ConversionError(
            "target type $(typestring(T)) has field $(fieldname(T, k)) with no field in the reader record and no default"))
        defaults[k] = Missing <: ft ? missing : nothing
    end
    return true
end

# ---- decoding ---------------------------------------------------------------------------------------

# One typed value, counted like `decode`. A leaf decodes in line; other nodes go through `nodevalue`.
@inline function decodenode(::Type{T}, @nospecialize(node::TypedNode), d::Decoder) where {T}
    countvalue!(d)
    leaftarget(T) && node isa LeafNode{T} && return leafvalue(node, d)
    return nodevalue(T, node, d)
end

# The value of a node for `T`, through the node kinds a target of `T`'s shape can have (the conditions on `T`
# fold where it compiles). A target with a null member also takes the nodes of its non-null member `X`, which
# decode schemas that never produce a null.
function nodevalue(::Type{T}, @nospecialize(node::TypedNode), d::Decoder) where {T}
    if T === Any
        node isa GenericNode{Any} && return decodevalue(node.plan, d)
        nodeerror()
    end
    if T isa Union
        N, X = splitoptional(T)
        if X !== nothing
            node isa NullableNode{T,X} && return nullablevalue(node, d)
            node isa LeafNode{N} && return leafvalue(node, d)
        end
        node isa UnionNode{T} && return unionvalue(node, d)
        node isa SemanticNode{T} && return semanticnodevalue(node, d)::T
        X === nothing && nodeerror()
        return nodevalue(X, node, d)
    end
    leaftarget(T) && node isa LeafNode{T} && return leafvalue(node, d)
    if isconcretetype(T)
        T <: Vector && node isa ArrayNode{T,eltype(T)} && return arrayvalue(node, d)
        T <: Dict && node isa MapNode{T,keytype(T),valtype(T)} && return mapvalue(node, d)
        recordlike(T) && node isa RecordNode{T} && return recordvalue(node, d)
    end
    T === Symbol && node isa SymbolNode && return targetsymbol(readstring(d))
    isenumtarget(T) && node isa EnumNode{T} && return enumvalue(node, d)
    node isa NullableNode{T,T} && return nullablevalue(node, d)
    node isa UnionNode{T} && return unionvalue(node, d)
    T === Record && node isa GenericNode{Record} && return decodevalue(node.plan, d)::Record
    node isa SemanticNode{T} && return semanticnodevalue(node, d)::T
    nodeerror()
end

# The types a `LeafNode` decodes into: the generic leaf types and the targets `convertleaf` converts to.
function leaftarget(@nospecialize(T))
    T isa DataType || return false
    isleaftype(T) && return true
    T <: Integer && return isbitstype(T)
    T <: Tuple && return all(t -> t === UInt8, T.parameters)
    return T === Nothing || T === Float16 || T === Char || T === DateTime
end

@noinline function nodeerror()
    throw(ArgumentError("internal error: a typed node does not decode its target type"))
end

# The leaf node's plan decoded into `T`, through the plans whose generic value is a `T` or converts to one.
@generated function leafvalue(node::LeafNode{T}, d::Decoder) where {T}
    body = Expr(:block, Expr(:meta, :inline), :(p = node.plan))
    for (P, V) in LEAF_PLAN_VALUES
        if V === T
            push!(body.args, :(p isa $P && return decodeleaf(p::$P, d)::$T))
        elseif leafconverts(T, P)
            push!(body.args, :(p isa $P && return convertleaf($T, decodeleaf(p::$P, d))::$T))
        end
    end
    T in PROMOTION_TARGETS && push!(body.args, :(p isa PromotePlan && return promotedvalue($T, p::PromotePlan, d)))
    T === EnumValue &&
        push!(body.args, :(p isa EnumRemapPlan && return EnumValue(p.reader, enumremapindex(p, d), Val(:unchecked))))
    push!(body.args, :(nodeerror()))
    return body
end

function targetsymbol(s::String)
    occursin('\0', s) && throw(ConversionError("a string holding a NUL cannot become a Symbol"))
    return Symbol(s)
end

function enumvalue(node::EnumNode{T}, d::Decoder) where {T}
    p = node.plan
    i = p isa EnumRemapPlan ? Int(enumremapindex(p, d)) : readindex(d, length(node.schema.symbols))
    m = node.members[i]
    m === nothing && throw(ConversionError(
        "target enum $(typestring(T)) has no member for $(repr(diagnostictext(node.schema.symbols[i])))"))
    return m
end

function nullablevalue(node::NullableNode{T,X}, d::Decoder) where {T,X}
    i = readindex(d, length(node.branches))
    b = node.branches[i]
    b === nothing || return decodenode(X, b, d)
    countvalue!(d)
    p = node.plans[i]
    p isa UnresolvableBranch && throw(ResolutionError(p.msg, p.writerpath, p.readerpath))
    return nullof(T)
end

# The null `T` admits.
function nullof(::Type{T}) where {T}
    Missing <: T && return missing
    Nothing <: T && return nothing
    throw(ConversionError("null is not accepted by the target type"))
end

@generated function unionvalue(node::UnionNode{T}, d::Decoder) where {T}
    body = Expr(:block, :(n = length(node.branches)), :(n == 0 && dataerror(d, "empty union has no datum")),
                :(i = readindex(d, n)), :(b = node.branches[i]), :(m = node.members[i]))
    for (k, M) in enumerate(T isa Union ? Base.uniontypes(T) : Any[T])
        push!(body.args, :(m == $k && return decodenode($M, b, d)))
    end
    push!(body.args, :(nodeerror()))
    return body
end

# The item closures capture the node, not its abstract child: a closure's type follows the run-time types of
# what it captures, so capturing the child would build and dispatch a new closure type per value.
function arrayvalue(node::ArrayNode{T,E}, d::Decoder) where {T,E}
    enter!(d)
    out = E[]
    eachblockitem(dd -> push!(out, decodenode(E, node.items, dd)), out, d, node.minsize, "array")
    leave!(d)
    return out
end

function mapvalue(node::MapNode{T,K,V}, d::Decoder) where {T,K,V}
    enter!(d)
    out = Dict{K,V}()
    eachblockitem(out, d, satadd(node.minsize, 1), "map") do dd
        key = readstring(dd)
        out[K === Symbol ? targetsymbol(key) : key] = decodenode(V, node.values, dd)
    end
    leave!(d)
    return out
end

function recordvalue(node::RecordNode{T}, d::Decoder) where {T}
    enter!(d)
    v = recordfields(node, d)
    leave!(d)
    return v
end

# The record built from its fields: straight-line in field order when the wire steps align with the target's
# fields, otherwise through the wire steps (a binary decision on each step's target field), skipping and
# defaulting as the node says.
@generated function recordfields(node::RecordNode{T}, d::Decoder) where {T}
    n = fieldcount(T)
    names = [Symbol(:f, k) for k in 1:n]
    types = [fieldtype(T, k) for k in 1:n]
    assign(k, j) = :($(names[k]) = decodenode($(types[k]), childnode(node.nodes, $j), d))
    function stepto(lo, hi)
        lo == hi && return assign(lo, :j)
        mid = (lo + hi) >>> 1
        return Expr(:if, :(slot <= $mid), stepto(lo, mid), stepto(mid + 1, hi))
    end
    step = n == 0 ? :(skip(node.plans[j], d)) : :(slot == 0 ? skip(node.plans[j], d) : $(stepto(1, n)))
    construct = T <: NamedTuple ? :($T(($(names...),))) : :($T($(names...)))
    return quote
        $([:(local $(names[k])::$(types[k])) for k in 1:n]...)
        if node.aligned
            $([assign(k, k) for k in 1:n]...)
        else
            for j in eachindex(node.slots)
                slot = node.slots[j]
                $step
            end
            $([:(node.filled[$k] || ($(names[k]) = freshvalue(node.defaults[$k]::$(types[k])))) for k in 1:n]...)
        end
        return $construct
    end
end

# A record step's node. The `nothing` check narrows the element to `TypedNode`; asserting that abstract
# parametric type instead would run a subtype check per field.
@inline function childnode(nodes::Vector{Union{Nothing,TypedNode}}, j::Int)
    node = @inbounds nodes[j]
    node === nothing && nodeerror()
    return node
end

# The semantic route: a generic decode converted through StructUtils, dispatched at run time (and compiled
# once, whatever the target).
Base.@nospecializeinfer function semanticnodevalue(@nospecialize(node::SemanticNode), d::Decoder)
    try
        return runtimecall(semanticdecode, node, d)
    catch err
        notcompiled(err, semanticdecode) && throw(ArgumentError(
            "a target here converts through StructUtils, which programs compiled by `juliac --trim` leave out; " *
            "decode into types whose fields Avro decodes directly"))
        rethrow()
    end
end

function semanticdecode(node::SemanticNode{T}, d::Decoder) where {T}
    return semanticvalue(T, decodevalue(node.plan, d), node.schema)
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
