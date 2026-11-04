# Typed encoding: a write node encodes values of a Julia type `T` under one schema node. Node types are
# parametric only on Julia types, so typed writing compiles once per value type, whatever the schema; a node
# reaches its plan through closed `isa` chains and the plans' own `encodevalue` methods, so its bytes,
# validation, value counts and union branches are those of the generic route. The typed route keeps no
# value path: a datum that fails is encoded again through the generic route (`encodedatum!`), whose error
# locates the value. Values the typed route does not cover (`Any` and other abstract types, generic values,
# unions of several types, rows read through their properties, iterables other than vectors) encode
# generically through `runtimecall`, which programs compiled by `juliac --trim` leave out.

abstract type WriteNode{T} end

# The generic route: `encodevalue(plan, e, x)` dispatched on the value's run-time type.
struct GenericWriteNode{T} <: WriteNode{T}
    plan::Plan
end

# A leaf plan's `encodevalue` method for a `T`.
struct LeafWriteNode{T} <: WriteNode{T}
    plan::Plan
end

# A two-branch nullable union: `missing` and `nothing` take the null branch, and `X` values (the members of
# `T` that are not null) the other branch through `value`. `value` is `nothing` when `T` has no such member,
# or when `X` is a `Base.Enum`, whose branch the union's method chooses by symbol.
struct NullableWriteNode{T,X} <: WriteNode{T}
    plan::WUnion
    value::Union{Nothing,WriteNode}
end

# A `T` with null members under a schema without a null branch: `X` values through `value`, and nulls as the
# generic route encodes them.
struct OptionalWriteNode{T,X} <: WriteNode{T}
    plan::Plan
    value::WriteNode
end

# An `AbstractVector` under an array schema.
struct ArrayWriteNode{T} <: WriteNode{T}
    plan::WArray
    items::WriteNode
end

# An `AbstractDict` keyed by strings or symbols under a map schema.
struct MapWriteNode{T} <: WriteNode{T}
    plan::WMap
    values::WriteNode
end

# A NamedTuple or plain struct under a record schema: schema field `k` encodes Julia field `positions[k]`
# (0 when `T` has none) through `nodes[k]`. Mutable, so a recursive record refers to its own node while it
# is built.
mutable struct RecordWriteNode{T} <: WriteNode{T}
    const plan::WRecord
    const nodes::Vector{WriteNode}
    const positions::Vector{Int}
    aligned::Bool                     # schema field `k` is Julia field `k` for every field, and there are no others
    fast::Bool                        # aligned, and every leaf field's plan is the one its type tries first
end

# ---- the shapes of value types (generated functions read these, so they come first) -------------------

# `(hasnull, X)`: whether `T` has `Missing` or `Nothing` as a member, and its other members `X` (`Union{}`
# when there are none, `nothing` when there are several). Generated, so both are constants where `T` is.
@generated function writesplit(::Type{T}) where {T}
    X = Base.nonnothingtype(Base.nonmissingtype(T))
    hasnull = X !== T
    return :(($hasnull, $(X isa Union ? nothing : X)))
end

# Types whose values a record node may encode from their fields (others encode generically): NamedTuples and
# plain structs, but not rows, whose fields are not their columns. A struct that overrides `propertynames` is
# found only at run time (`usesproperties`).
function recordshape(@nospecialize(T))
    T <: Union{Tables.AbstractRow,TableRow} && return false
    return T <: NamedTuple || (isstructtype(T) && !(T <: NOT_RECORDLIKE))
end

function recordtarget(@nospecialize(T))
    return recordshape(T) && isconcretetype(T) && !usesproperties(T)
end

function isleafplan(@nospecialize(p::Plan))
    return !(p isa Union{WRecord,WArray,WMap,WUnion})
end

# Whether `T` is concrete, or concrete apart from null members: the types whose values typed nodes can encode.
@generated function typedvalue(::Type{T}) where {T}
    X = Base.nonnothingtype(Base.nonmissingtype(T))
    return X === Union{} || isconcretetype(X)
end

# How many plan types a leaf tries inline, and the widest record encoded field by field inline (wider ones make
# a call per field).
const LEAF_INLINE_PLANS = 1
const TYPED_RECORD_FIELDS = 32

# The leaf plan types, each value type's own plans first, then those of its kind (`leafplanorder`).
const LEAF_WRITE_PLANS = (LongPlan, StringPlan, DoublePlan, IntPlan, BoolPlan, FloatPlan, BytesPlan, NullPlan,
    EnumPlan, FixedPlan, DatePlan, TimeMillisPlan, TimeMicrosPlan, TimestampPlan{Millisecond},
    TimestampPlan{Microsecond}, TimestampPlan{Nanosecond}, LocalTimestampPlan{Millisecond},
    LocalTimestampPlan{Microsecond}, LocalTimestampPlan{Nanosecond}, DecimalPlan, UUIDStringPlan, UUIDFixedPlan,
    DurationPlan)

# The order a leaf of `T` tries the plan types in: only the first match runs, so the order costs a few
# comparisons when the schema is not the one `T` reads as, and never changes the result.
function leafplanorder(@nospecialize(T))
    order = Any[]
    for (P, V) in LEAF_PLAN_VALUES
        V === T && !(P in order) && push!(order, P)
    end
    kin = T <: Bool ? (BoolPlan,) :
          T <: Integer ? (LongPlan, IntPlan, DoublePlan, FloatPlan) :
          T <: AbstractFloat ? (DoublePlan, FloatPlan) :
          T <: Union{AbstractString,Symbol,Char} ? (StringPlan, EnumPlan, UUIDStringPlan) :
          T <: Union{AbstractVector{UInt8},Tuple} ? (BytesPlan, FixedPlan) :
          T <: DateTime ? (LocalTimestampPlan{Millisecond}, TimestampPlan{Millisecond}) :
          T <: Base.Enum ? (EnumPlan,) : ()
    for P in (kin..., LEAF_WRITE_PLANS...)
        P in order || push!(order, P)
    end
    return order
end

# The leaf plan types without data, which a record encodes inline from a code alone (`leafcode`).
const SINGLETON_LEAF_PLANS = (LongPlan, StringPlan, DoublePlan, IntPlan, BoolPlan, FloatPlan, BytesPlan, NullPlan,
    DatePlan, TimeMillisPlan, TimeMicrosPlan, TimestampPlan{Millisecond}, TimestampPlan{Microsecond},
    TimestampPlan{Nanosecond}, LocalTimestampPlan{Millisecond}, LocalTimestampPlan{Microsecond},
    LocalTimestampPlan{Nanosecond}, UUIDStringPlan, UUIDFixedPlan, DurationPlan)

# The position in `SINGLETON_LEAF_PLANS` of a leaf node's plan, or 0.
Base.@nospecializeinfer function leafcode(@nospecialize(node::WriteNode))
    node isa LeafWriteNode || return 0x00
    p = node.plan
    for (i, P) in enumerate(SINGLETON_LEAF_PLANS)
        p isa P && return UInt8(i)
    end
    return 0x00
end

# The code and plan type a record field of type `T` checks first, or `nothing` for a type that is not a leaf.
function leaffastpath(@nospecialize(T))
    (isconcretetype(T) && !recordshape(T)) || return nothing
    P = first(leafplanorder(T))
    i = findfirst(x -> x === P, SINGLETON_LEAF_PLANS)
    return i === nothing ? nothing : (UInt8(i), P)
end

# The distinct field types of `T`, and the position among them of each field's type: generated code branches
# on a field's type rather than on the field, so it stays small for a record of many fields of few types.
function fieldtypegroups(@nospecialize(T))
    types = Any[]
    index = Int[]
    for i in 1:fieldcount(T)
        ft = fieldtype(T, i)
        t = findfirst(x -> x === ft, types)
        t === nothing && (push!(types, ft); t = length(types))
        push!(index, t)
    end
    return types, index
end

# ---- construction -----------------------------------------------------------------------------------

# The record nodes built so far, by (write plan, Julia type).
const WriteMemo = IdDict{Any,WriteNode}

"The write node encoding `T` values under `schema` and its write plan."
Base.@nospecializeinfer function writenode(::Type{T}, @nospecialize(schema::Schema), @nospecialize(plan::Plan)) where {T}
    return buildwritenode(T, schema, plan, WriteMemo())
end

Base.@nospecializeinfer function buildwritenode(::Type{T}, @nospecialize(s::Schema), @nospecialize(p::Plan), memo::WriteMemo) where {T}
    hasnull, X = writesplit(T)
    X === nothing && return GenericWriteNode{T}(p)                 # several members choose a branch by value
    (X === Union{} || isconcretetype(X)) || return GenericWriteNode{T}(p)
    X !== Union{} && X <: Union{Record,UnionValue} && return GenericWriteNode{T}(p)
    if p isa WUnion
        p.nullable == 0 && return GenericWriteNode{T}(p)
        (X === Union{} || X <: Base.Enum) && return NullableWriteNode{T,X}(p, nothing)
        u = s::UnionSchema
        b = 3 - p.nullable
        return NullableWriteNode{T,X}(p, buildwritenode(X, u.branches[b], p.branches[b], memo))
    end
    if hasnull
        X === Union{} && return isconcretetype(T) && isleafplan(p) ? LeafWriteNode{T}(p) : GenericWriteNode{T}(p)
        return OptionalWriteNode{T,X}(p, buildwritenode(X, s, p, memo))
    end
    p isa WRecord && return recordtarget(T) ? buildrecordwritenode(T, p, memo) : GenericWriteNode{T}(p)
    if p isa WArray
        T <: AbstractVector || return GenericWriteNode{T}(p)
        return ArrayWriteNode{T}(p, buildwritenode(eltype(T), (s::ArraySchema).items, p.items, memo))
    end
    if p isa WMap
        (T <: AbstractDict && keytype(T) <: Union{AbstractString,Symbol}) || return GenericWriteNode{T}(p)
        return MapWriteNode{T}(p, buildwritenode(valtype(T), (s::MapSchema).values, p.values, memo))
    end
    return LeafWriteNode{T}(p)
end

# The schema node and plan a field's node is built for. The generated builder takes it whole: a generated
# function specializes on every argument's run-time type, and this one is concrete.
struct WriteSite
    schema::Schema
    plan::Plan
    memo::WriteMemo
end

function buildrecordwritenode(::Type{T}, p::WRecord, memo::WriteMemo) where {T}
    cached = get(memo, (p, T), nothing)
    cached === nothing || return cached
    positions = recordfieldpositions(p.schema, T)
    node = RecordWriteNode{T}(p, WriteNode[], positions, positions == 1:fieldcount(T), false)
    memo[(p, T)] = node                                # recursive references resolve to this node
    for (k, f) in enumerate(p.schema.fields)
        j = positions[k]
        push!(node.nodes, j == 0 ? GenericWriteNode{Any}(p.fields[k]) : fieldwritenode(T, j, WriteSite(f.schema, p.fields[k], memo)))
    end
    if node.aligned && fieldcount(T) <= TYPED_RECORD_FIELDS
        expected = fastleafcodes(T)
        node.fast = true
        for k in eachindex(node.nodes)
            expected[k] == 0x00 || leafcode(node.nodes[k]) == expected[k] || (node.fast = false)
        end
    end
    return node
end

# Each field's `leaffastpath` code (0 for a field type without one), known when the method compiles.
@generated function fastleafcodes(::Type{T}) where {T}
    codes = UInt8[(f = leaffastpath(fieldtype(T, j)); f === nothing ? 0x00 : f[1]) for j in 1:fieldcount(T)]
    return :($(Tuple(codes)))
end

# The node encoding Julia field `j` of `T`, built for the field's constant type.
@generated function fieldwritenode(::Type{T}, j::Int, site::WriteSite) where {T}
    types, index = fieldtypegroups(T)
    body = Expr(:block, :(t = $(Tuple(index))[j]))
    for (t, FT) in enumerate(types)
        push!(body.args, :(t == $t && return buildwritenode($FT, site.schema, site.plan, site.memo)))
    end
    push!(body.args, :(throw(ArgumentError("internal error: no field $j in the value type"))))
    return body
end

# The write nodes of one schema by value type, for writers that take datums of any type: the last type's node
# is kept at hand, since a writer mostly sees one.
mutable struct WriteNodes
    const schema::Schema
    const plan::Plan
    lasttype::Any
    lastnode::Union{Nothing,WriteNode}
    const nodes::IdDict{Any,WriteNode}
end

function WriteNodes(@nospecialize(schema::Schema), @nospecialize(plan::Plan))
    return WriteNodes(schema, plan, nothing, nothing, IdDict{Any,WriteNode}())
end

# The `nothing` checks narrow without the subtype test an assertion to the abstract `WriteNode` would run.
function writenode!(c::WriteNodes, ::Type{T}) where {T}
    if c.lasttype === T
        last = c.lastnode
        last === nothing || return last
    end
    node = get(c.nodes, T, nothing)
    if node === nothing
        node = writenode(T, c.schema, c.plan)
        c.nodes[T] = node
    end
    c.lasttype = T
    c.lastnode = node
    return node
end

# ---- encoding ---------------------------------------------------------------------------------------

# One value of `T`, counted like `encode`. `fast` records and leaves, the common kinds, encode inline; the other
# kinds through one call to `nodeencode`. A `T` with abstract members has a generic node, which takes the value
# as it is: a call specialized on it would dispatch at run time.
@inline function encodenode(::Type{T}, @nospecialize(node::WriteNode), e::Encoder, x) where {T}
    countvalue!(e)
    if !typedvalue(T)
        node isa GenericWriteNode{T} || writenodeerror()
        genericencode(node, e, x)
    elseif recordshape(T) && node isa RecordWriteNode{T} && node.fast
        enter!(e)
        fastfields(node, e, x::T)
        leave!(e)
    elseif isconcretetype(T) && !recordshape(T) && node isa LeafWriteNode{T}
        leafencode(node, e, x::T)
    else
        nodeencode(T, node, e, x)
    end
    return nothing
end

# A value of `T` through the node kinds a value of `T`'s shape can have (the conditions on `T` fold where it
# compiles).
function nodeencode(::Type{T}, @nospecialize(node::WriteNode), e::Encoder, x) where {T}
    hasnull, X = writesplit(T)
    if X !== nothing && !hasnull && isconcretetype(T)
        if recordshape(T)
            node isa RecordWriteNode{T} && return recordencode(node, e, x::T)
        elseif T <: AbstractVector
            node isa ArrayWriteNode{T} && return arrayencode(node, e, x::T)
        elseif T <: AbstractDict
            node isa MapWriteNode{T} && return mapencode(node, e, x::T)
        end
        node isa LeafWriteNode{T} && return leafencode(node, e, x::T)
        node isa NullableWriteNode{T,T} && return nullableencode(node, e, x::T)
    elseif X !== nothing
        node isa NullableWriteNode{T,X} && return nullableencode(node, e, x)
        node isa OptionalWriteNode{T,X} && return optionalencode(node, e, x)
        isconcretetype(T) && node isa LeafWriteNode{T} && return leafencode(node, e, x::T)
    end
    node isa GenericWriteNode{T} && return genericencode(node, e, x)
    writenodeerror()
end

@noinline function writenodeerror()
    throw(ArgumentError("internal error: a write node does not encode its value type"))
end

# The leaf node's plan through a chain over every leaf plan type: each call resolves to that plan's method
# for `T`, or to the error of a plan that does not take a `T`. The first `LEAF_INLINE_PLANS` plans of
# `leafplanorder` are tried inline and the rest out of line, so a record of many fields stays small.
@generated function leafencode(node::LeafWriteNode{T}, e::Encoder, x::T) where {T}
    body = Expr(:block, Expr(:meta, :inline), :(p = node.plan))
    for P in leafplanorder(T)[1:LEAF_INLINE_PLANS]
        push!(body.args, :(p isa $P && return encodevalue(p::$P, e, x)))
    end
    push!(body.args, :(return leafencoderest(node, e, x)))
    return body
end

@noinline @generated function leafencoderest(node::LeafWriteNode{T}, e::Encoder, x::T) where {T}
    body = Expr(:block, :(p = node.plan))
    for P in leafplanorder(T)[LEAF_INLINE_PLANS + 1:end]
        push!(body.args, :(p isa $P && return encodevalue(p::$P, e, x)))
    end
    push!(body.args, :(writenodeerror()))
    return body
end

function nullableencode(node::NullableWriteNode{T,X}, e::Encoder, x) where {T,X}
    p = node.plan
    if x === missing || x === nothing
        writelong!(e, Int64(p.nullable - 1))
        countvalue!(e)                                  # the null branch's value, as `encode` counts it
        return nothing
    end
    X !== Union{} && X <: Base.Enum && return encodevalue(p, e, x::X)
    writelong!(e, Int64(2 - p.nullable))                # the other branch
    value = node.value
    value === nothing && writenodeerror()
    return encodenode(X, value, e, x::X)
end

function optionalencode(node::OptionalWriteNode{T,X}, e::Encoder, x) where {T,X}
    (x === missing || x === nothing) && return encodenull(node.plan, e, x)
    return nodeencode(X, node.value, e, x::X)           # counted as `T`'s value
end

# A null under a schema without a null branch, as the generic route encodes it: nothing to write for `null`,
# the plan's error for another leaf, and the composite plans' own handling.
Base.@nospecializeinfer function encodenull(@nospecialize(p::Plan), e::Encoder, x::Union{Missing,Nothing})
    p isa NullPlan && return nothing
    p isa Union{WRecord,WArray,WMap} && return genericvalue(p, e, x)
    return encodeerror(expected(p), x)
end

# A sized vector is one block; one of unknown size (by its `IteratorSize`) one block per item.
function arrayencode(node::ArrayWriteNode{T}, e::Encoder, x::T) where {T}
    enter!(e)
    items = node.items
    if Base.IteratorSize(T) isa Union{Base.HasLength,Base.HasShape}
        n = length(x)
        n > 0 && writelong!(e, Int64(n))
        actual = 0
        for v in x
            actual += 1
            actual <= n || arraylengtherror(x, n, -1)
            encodenode(eltype(T), items, e, v)
        end
        actual == n || arraylengtherror(x, n, actual)
    else
        for v in x
            writelong!(e, Int64(1))
            encodenode(eltype(T), items, e, v)
        end
    end
    writelong!(e, Int64(0))
    leave!(e)
    return nothing
end

@noinline function arraylengtherror(x, n::Int, actual::Int)
    actual < 0 && encodeerror(string("array iterator produced more than its declared length ", n), x)
    encodeerror(string("array iterator produced ", actual, " values but declared length ", n), x)
end

function mapencode(node::MapWriteNode{T}, e::Encoder, x::T) where {T}
    enter!(e)
    n = length(x)
    if n > 0
        writelong!(e, Int64(n))
        seen = distinctkeys(x) ? nothing : Set{String}()
        values = node.values
        i = 0
        for (k, v) in x
            i += 1
            i <= n || encodeerror("map size changed during iteration", x)
            key = mapkeystring(k)
            if seen !== nothing
                key in seen && encodeerror("map keys are duplicates after conversion to strings", x)
                push!(seen, key)
            end
            writestring!(e, key)
            encodenode(valtype(T), values, e, v)
        end
        i == n || encodeerror("map size changed during iteration", x)
    end
    writelong!(e, Int64(0))
    leave!(e)
    return nothing
end

function recordencode(node::RecordWriteNode{T}, e::Encoder, x::T) where {T}
    enter!(e)
    node.fast ? fastfields(node, e, x) : slowfields(node, e, x)
    leave!(e)
    return nothing
end

# The fields of a `fast` record: each leaf field's plan is a constant, as for a schema known when compiling.
@generated function fastfields(node::RecordWriteNode{T}, e::Encoder, x::T) where {T}
    fieldcount(T) <= TYPED_RECORD_FIELDS || return :(writenodeerror())
    function known(j)
        FT = fieldtype(T, j)
        fast = leaffastpath(FT)
        fast === nothing && return :(encodenode($FT, @inbounds(nodes[$j]), e, getfield(x, $j)))
        return :(countvalue!(e); encodevalue($(fast[2])(), e, getfield(x, $j)))
    end
    return quote
        $(Expr(:meta, :inline))
        nodes = node.nodes
        $([known(j) for j in 1:fieldcount(T)]...)
        return nothing
    end
end

# Straight-line when the fields are `T`'s in order, otherwise by position.
@generated function slowfields(node::RecordWriteNode{T}, e::Encoder, x::T) where {T}
    straight = [:(encodenode($(fieldtype(T, j)), @inbounds(nodes[$j]), e, getfield(x, $j))) for j in 1:fieldcount(T)]
    return quote
        $(Expr(:meta, :noinline))
        node.aligned || return fieldsbyposition(node, e, x)
        nodes = node.nodes
        $(straight...)
        return nothing
    end
end

# Each schema field through a branch on its Julia field's type and a call.
@generated function fieldsbyposition(node::RecordWriteNode{T}, e::Encoder, x::T) where {T}
    types, index = fieldtypegroups(T)
    bytype = Expr(:block, :(t = $(Tuple(index))[j]))
    for (t, FT) in enumerate(types)
        push!(bytype.args, :(t == $t && (encodefield($FT, @inbounds(nodes[k]), e, typedfield(x, j, $FT)); continue)))
    end
    return quote
        $(Expr(:meta, :noinline))
        nodes = node.nodes
        positions = node.positions
        for k in eachindex(positions)
            j = @inbounds positions[k]
            j == 0 && missingfield(node, x, k)
            $bytype
        end
        return nothing
    end
end

# Field `j` of `x`, one of its fields of type `FT`, through a binary decision over those fields.
@generated function typedfield(x::T, j::Int, ::Type{FT}) where {T,FT}
    fields = [i for i in 1:fieldcount(T) if fieldtype(T, i) === FT]
    function stepto(lo, hi)
        lo == hi && return :(getfield(x, $(fields[lo])))
        mid = (lo + hi) >>> 1
        return Expr(:if, :(j <= $(fields[mid])), stepto(lo, mid), stepto(mid + 1, hi))
    end
    isempty(fields) && return :(throw(ArgumentError("internal error: no field $j of that type")))
    return Expr(:block, Expr(:meta, :inline), stepto(1, length(fields)))
end

@noinline function encodefield(::Type{T}, @nospecialize(node::WriteNode), e::Encoder, x) where {T}
    encodenode(T, node, e, x)
    return nothing
end

@noinline function missingfield(node::RecordWriteNode{T}, x, k::Int) where {T}
    p = node.plan
    encodeerror(missingfieldmessage(typestring(T), p.schema.fields[k].name, fullname(p.schema)), x)
end

@noinline Base.@constprop :none function missingfieldmessage(type::String, field::String, record::String)
    return string(type, " has no field for \"", field, "\" of record ", record)
end

@noinline function genericencode(node::GenericWriteNode, e::Encoder, @nospecialize(x))
    return genericvalue(node.plan, e, x)
end

# `encodevalue(p, e, x)` dispatched at run time, so that `juliac --trim` neither compiles nor verifies the
# generic route; a trimmed program raises an `ArgumentError` naming the route instead.
Base.@nospecializeinfer function genericvalue(@nospecialize(p::Plan), e::Encoder, @nospecialize(x))
    try
        runtimecall(encodevalue, p, e, x)
    catch err
        notcompiled(err, encodevalue) && throw(ArgumentError(
            "a value here encodes by its run-time type, which programs compiled by `juliac --trim` leave out; " *
            "write NamedTuples, structs, vectors and dictionaries of types Avro encodes directly"))
        rethrow()
    end
    return nothing
end

# ---- datums -----------------------------------------------------------------------------------------

# The datum `x`, of type `T`, encoded through `node` from depth zero onto `e`. A failure leaves `e`'s bytes,
# counts and state as they were; an `EncodeError` is raised again by the generic route, whose error locates
# the value.
@inline function encodetyped!(::Type{T}, @nospecialize(node::WriteNode), e::Encoder, x, @nospecialize(plan::Plan),
                              @nospecialize(schema::Schema)) where {T}
    start, values, depth = e.pos, e.values, e.depth
    e.depth = 0
    try
        rootencode(T, node, e, x)
    catch err
        e.pos, e.values, e.depth, e.expected = start, values, depth, nothing
        empty!(e.path)
        err isa EncodeError && replaydatum(plan, e.limits, x, schema)
        rethrow()
    end
    e.depth = depth
    return nothing
end

# The datum's own encoding, out of line: the frame that holds the exception handler stays small.
@noinline function rootencode(::Type{T}, @nospecialize(node::WriteNode), e::Encoder, x) where {T}
    encodenode(T, node, e, x)
    return nothing
end

# Encode the datum again through the generic route, which raises its error with the value's path; returns
# when that route is not compiled (`juliac --trim`), so the typed route's error stands. Specialized on the
# datum's type: boxing a datum for the call from a `catch` block would allocate for every datum.
@noinline function replaydatum(@nospecialize(plan::Plan), limits::Limits, x, @nospecialize(schema::Schema))
    try
        runtimecall(encodedatum!, plan, Encoder(limits), x, schema)
    catch err
        notcompiled(err, encodedatum!) && return nothing
        rethrow()
    end
    return nothing
end
