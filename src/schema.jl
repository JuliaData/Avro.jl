# Schema model (plan §4.2): immutable, transitively frozen schema graphs with dense node ids, the
# fullname algorithm, the contextual attribute grammar, the recursive default rule, printing and
# structural equality. Nodes are heap objects with `const` fields (never inlined into the values that
# reference them: `Record`, `EnumValue` and `Fixed` carry one 8-byte reference — plan §4.4 (b)).

const Props = FrozenDict{String,Any}   # custom attributes as frozen JSON trees (raw number tokens)

"""
    GraphInfo

Per-graph facts filled by `freeze!`: the `Limits` the graph was admitted under, the repair flags, and the
node/named-type counts.
"""
struct GraphInfo
    limits::Limits
    repaired_names::Bool
    repaired_defaults::Bool
    nodes::Int
    namedtypes::Int
end

"""
    NodeMeta

Write-once node identity: a dense graph-local `id`, the shared `GraphInfo`, and the structural `hash`.
"""
struct NodeMeta
    id::FrozenRef{Int32}
    graph::FrozenRef{GraphInfo}
    hash::FrozenRef{UInt64}
end

function NodeMeta()
    return NodeMeta(FrozenRef{Int32}(), FrozenRef{GraphInfo}(), FrozenRef{UInt64}())
end

abstract type Schema end

struct NoDefault end

"""
    Avro.nodefault

The sentinel for "no default" on fields and enums (`nothing`/`missing` as a default mean a JSON `null`).
"""
const nodefault = NoDefault()

"""
    DefaultValue(json, branch, span, index, valid)

A parsed default: its frozen JSON tree, the selected union branch (1-based; 0 when not a union), an
owned copy of the exact source text, the enum symbol index (1-based; 0 otherwise) and whether it
validated against the schema (invalid defaults are kept only under `allow_invalid_defaults=true`).
"""
struct DefaultValue
    json::Any
    branch::Int
    span::String
    index::Int
    valid::Bool
end

const Default = Union{NoDefault,DefaultValue}

mutable struct NullSchema <: Schema; const props::Props; const meta::NodeMeta; end
mutable struct BooleanSchema <: Schema; const props::Props; const meta::NodeMeta; end
mutable struct IntSchema <: Schema; const logical::Union{Nothing,LogicalType}; const props::Props; const meta::NodeMeta; end
mutable struct LongSchema <: Schema; const logical::Union{Nothing,LogicalType}; const props::Props; const meta::NodeMeta; end
mutable struct FloatSchema <: Schema; const props::Props; const meta::NodeMeta; end
mutable struct DoubleSchema <: Schema; const props::Props; const meta::NodeMeta; end
mutable struct BytesSchema <: Schema; const logical::Union{Nothing,LogicalType}; const props::Props; const meta::NodeMeta; end
mutable struct StringSchema <: Schema; const logical::Union{Nothing,LogicalType}; const props::Props; const meta::NodeMeta; end
mutable struct ArraySchema <: Schema; const items::Schema; const props::Props; const meta::NodeMeta; end
mutable struct MapSchema <: Schema; const values::Schema; const props::Props; const meta::NodeMeta; end
mutable struct UnionSchema <: Schema; const branches::FrozenVector{Schema}; const meta::NodeMeta; end

mutable struct FixedSchema <: Schema
    const name::FullName
    const aliases::FrozenVector{String}      # normalised fullnames
    const rawaliases::FrozenVector{String}   # as written, for re-emission
    const size::Int
    const logical::Union{Nothing,LogicalType}
    const props::Props
    const meta::NodeMeta
end

mutable struct EnumSchema <: Schema
    const name::FullName
    const aliases::FrozenVector{String}
    const rawaliases::FrozenVector{String}
    const doc::Union{Nothing,String}
    const symbols::FrozenVector{String}
    const default::Default
    const symbolindex::FrozenDict{String,Int}
    const props::Props
    const meta::NodeMeta
end

struct Field
    name::String
    schema::Schema
    doc::Union{Nothing,String}
    default::Default
    order::Symbol
    aliases::FrozenVector{String}
    props::Props
end

mutable struct RecordSchema <: Schema
    const name::FullName
    const aliases::FrozenVector{String}
    const rawaliases::FrozenVector{String}
    const doc::Union{Nothing,String}
    const iserror::Bool
    const props::Props
    const fields::FrozenVector{Field}              # filled after registration (self-references resolve), then frozen
    const fieldindex::FrozenDict{String,Int}
    const meta::NodeMeta
end

const NamedSchema = Union{RecordSchema,EnumSchema,FixedSchema}
const PrimitiveSchema = Union{NullSchema,BooleanSchema,IntSchema,LongSchema,FloatSchema,DoubleSchema,BytesSchema,StringSchema}

function kind(::NullSchema)
    return :null
end

function kind(::BooleanSchema)
    return :boolean
end

function kind(::IntSchema)
    return :int
end

function kind(::LongSchema)
    return :long
end

function kind(::FloatSchema)
    return :float
end

function kind(::DoubleSchema)
    return :double
end

function kind(::BytesSchema)
    return :bytes
end

function kind(::StringSchema)
    return :string
end

function kind(::ArraySchema)
    return :array
end

function kind(::MapSchema)
    return :map
end

function kind(::UnionSchema)
    return :union
end

function kind(::FixedSchema)
    return :fixed
end

function kind(::EnumSchema)
    return :enum
end

function kind(s::RecordSchema)
    return s.iserror ? :error : :record
end

function logical(s::Union{IntSchema,LongSchema,BytesSchema,StringSchema,FixedSchema})
    return s.logical
end

function logical(::Schema)
    return nothing
end

function props(s::UnionSchema)
    return Props()
end

function props(s::Schema)
    return s.props
end

"""
    Avro.fullname(schema::NamedSchema) -> String
"""
function fullname(s::NamedSchema)
    return fullname(s.name)
end

function nodeid(s::Schema)
    return s.meta.id[]
end

function graphinfo(s::Schema)
    return s.meta.graph[]
end

function Base.hash(s::Schema, h::UInt)
    return hash(s.meta.hash[], h)
end

"""
    Avro.Schema constructors: Avro.NullSchema(; props), …, Avro.RecordSchema(name; …), Avro.Field(name, schema; …)

Public constructors validate every §4.2 rule, reject `props` keys that collide with the structural keys
the same constructor emits, deep-copy already-frozen children into the new graph, and freeze.
"""
function NullSchema(; props=(;), limits::Limits=Limits())
    return build(NullSchema, props; limits=limits)
end

function BooleanSchema(; props=(;), limits::Limits=Limits())
    return build(BooleanSchema, props; limits=limits)
end

function FloatSchema(; props=(;), limits::Limits=Limits())
    return build(FloatSchema, props; limits=limits)
end

function DoubleSchema(; props=(;), limits::Limits=Limits())
    return build(DoubleSchema, props; limits=limits)
end

function IntSchema(; logical=nothing, props=(;), limits::Limits=Limits())
    return build(IntSchema, props; logical=logical, limits=limits)
end

function LongSchema(; logical=nothing, props=(;), limits::Limits=Limits())
    return build(LongSchema, props; logical=logical, limits=limits)
end

function BytesSchema(; logical=nothing, props=(;), limits::Limits=Limits())
    return build(BytesSchema, props; logical=logical, limits=limits)
end

function StringSchema(; logical=nothing, props=(;), limits::Limits=Limits())
    return build(StringSchema, props; logical=logical, limits=limits)
end

# ---- parse context -----------------------------------------------------------------------------

mutable struct ParseContext
    const limits::Limits
    const budget::Budget
    const allow_invalid_names::Bool
    const allow_invalid_defaults::Bool
    const named::FrozenDict{String,Schema}    # fullname → schema (sorted vector; §4.4 replacement growth)
    const pending::Vector{RecordSchema}       # records registered but not yet filled (≤ max_depth, prebuilt)
    metas::Vector{NodeMeta}                   # in creation order → dense ids (§4.4 replacement growth)
    metascap::Int
    const namedcount::Base.RefValue{Int}
    const legacyfixednames::Bool              # legacy=:avrojl1: Avro.jl ≤ 1.1.2 wrote fixed schemas without names
    legacyfixedcount::Int                     # nameless-fixed ordinal within this document
    repaired_names::Bool
    repaired_defaults::Bool
    depth::Int
end

function ParseContext(limits::Limits, budget::Budget, allow_invalid_names::Bool, allow_invalid_defaults::Bool, legacyfixednames::Bool=false)
    metascap = 16
    state = vectorbytes(RecordSchema, limits.max_schema_depth) + vectorbytes(NodeMeta, metascap) + 128   # pending, metas, context and table shells
    reserve!(budget, state)
    pending = Vector{RecordSchema}(undef, limits.max_schema_depth)   # the fill stack never exceeds max_schema_depth (§4.4)
    resize!(pending, 0)
    metas = Vector{NodeMeta}(undef, metascap)
    resize!(metas, 0)
    ctx = ParseContext(limits, budget, allow_invalid_names, allow_invalid_defaults, FrozenDict{String,Schema}(),
        pending, metas, metascap, Ref(0), legacyfixednames, 0, false, false, 0)
    allocated!(budget, state)
    return ctx
end

function schemaerror(msg::AbstractString, path::AbstractString)
    throw(SchemaError(String(msg), String(path)))
end

# Shared frozen empties: frozen containers are immutable by contract, so every node may reference the
# same empty instance instead of allocating one (uncharged: process-global constants, plan §4.6).
const EMPTY_PROPS = freeze!(Props())
const EMPTY_STRING_LIST = freeze!(FrozenVector{String}())

const NODE_META_BYTES = 96    # the NodeMeta object and its three write-once refs
const NODE_SHELL_BYTES = 64   # the schema node object, settled by `settlednode` after construction

function growmetas!(ctx::ParseContext)
    length(ctx.metas) == ctx.metascap || return nothing
    newcap = checked_mul(2, ctx.metascap)
    reserve!(ctx.budget, vectorbytes(NodeMeta, newcap))
    replacement = Vector{NodeMeta}(undef, newcap)
    allocated!(ctx.budget, vectorbytes(NodeMeta, newcap))
    resize!(replacement, length(ctx.metas))
    copyto!(replacement, 1, ctx.metas, 1, length(ctx.metas))
    oldbytes = vectorbytes(NodeMeta, ctx.metascap)
    ctx.metas = replacement                       # the old table is unreachable only after the rebind
    ctx.metascap = newcap
    release!(ctx.budget, oldbytes)
    return nothing
end

function newmeta!(ctx::ParseContext, path::AbstractString)
    length(ctx.metas) < ctx.limits.max_schema_nodes ||
        throw(LimitError(:max_schema_nodes, length(ctx.metas) + 1, ctx.limits.max_schema_nodes, :max_schema_nodes, :decode))
    growmetas!(ctx)
    reserve!(ctx.budget, NODE_META_BYTES + NODE_SHELL_BYTES)
    m = NodeMeta()
    push!(ctx.metas, m)
    allocated!(ctx.budget, NODE_META_BYTES)       # the node shell settles after the node is constructed
    return m
end

"Settle the node-shell reservation `newmeta!` made, right after the schema node exists (§4.4 order)."
function settlednode(ctx::ParseContext, s::Schema)
    allocated!(ctx.budget, NODE_SHELL_BYTES)
    return s
end

# ---- attribute grammar ---------------------------------------------------------------------------

const SCHEMA_GRAMMAR = Dict{Symbol,Tuple{Vararg{String}}}(
    :null => ("type",), :boolean => ("type",), :int => ("type",), :long => ("type",), :float => ("type",),
    :double => ("type",), :bytes => ("type",), :string => ("type",),
    :array => ("type", "items"), :map => ("type", "values"),
    :record => ("type", "name", "namespace", "aliases", "doc", "fields"),
    :error => ("type", "name", "namespace", "aliases", "doc", "fields"),
    :enum => ("type", "name", "namespace", "aliases", "doc", "symbols", "default"),
    :fixed => ("type", "name", "namespace", "aliases", "size"),
)
const FIELD_GRAMMAR = ("name", "type", "doc", "default", "order", "aliases")

function collectprops(ctx::ParseContext, obj::JSONObject, grammar, path::AbstractString)
    n = 0
    for k in obj.order
        k in grammar || (n += 1)
    end
    n == 0 && return EMPTY_PROPS
    slots = vectorbytes(String, n) + vectorbytes(Any, n) + 32
    reserve!(ctx.budget, slots)                    # the exact key/value capacity and dict shell (§4.4)
    p = emptywithcapacity(Props, n)
    allocated!(ctx.budget, slots)
    for k in obj.order
        k in grammar && continue
        retain!(ctx.budget, sizeof(k) + 32)        # the tree key and value this schema keeps alive
        p[k] = obj[k]
    end
    return freeze!(p)
end

function stringattr(obj::JSONObject, key::String, path::AbstractString; required::Bool=false)
    haskey(obj, key) || (required ? schemaerror("missing required attribute \"$key\"", path) : return nothing)
    v = obj[key]
    v isa String || schemaerror("attribute \"$key\" must be a JSON string", string(path, ".", key))
    return v
end

function stringarrayattr(ctx::ParseContext, obj::JSONObject, key::String, path::AbstractString; required::Bool=false)
    haskey(obj, key) || (required ? schemaerror("missing required attribute \"$key\"", path) : return nothing)
    v = obj[key]
    v isa JSONArray || schemaerror("attribute \"$key\" must be a JSON array of strings", string(path, ".", key))
    out = BuildBuf{String}(ctx.budget, length(v))
    for (i, x) in enumerate(v)
        x isa String || schemaerror("attribute \"$key\" must be a JSON array of strings", string(path, ".", key, "[", i - 1, "]"))
        push!(out, ctx.budget, x)
    end
    return finishbuild!(out, ctx.budget)
end

function checknamebytes(ctx::ParseContext, s::AbstractString, what::AbstractString, path::AbstractString)
    sizeof(s) <= ctx.limits.max_name_bytes ||
        throw(LimitError(:max_name_bytes, sizeof(s), ctx.limits.max_name_bytes, :max_name_bytes, :decode))
    return nothing
end

function checkname(ctx::ParseContext, s::AbstractString, what::AbstractString, path::AbstractString)
    checknamebytes(ctx, s, what, path)
    isvalidname(s) && return nothing
    ctx.allow_invalid_names || schemaerror("invalid $what \"$(escapename(s))\" (must match [A-Za-z_][A-Za-z0-9_]*)", path)
    ctx.repaired_names = true
    return nothing
end

function checknamespace(ctx::ParseContext, s::AbstractString, path::AbstractString)
    checknamebytes(ctx, s, "namespace", path)
    isvalidnamespace(s) && return nothing
    ctx.allow_invalid_names || schemaerror("invalid namespace \"$(escapename(s))\"", path)
    ctx.repaired_names = true
    return nothing
end

function escapename(s::AbstractString)
    return String(chop(sprint(escapejson, s); head=1, tail=1))   # character-wise: the quoted text may end in a multi-byte character
end

# ---- parsing ------------------------------------------------------------------------------------

"""
    Avro.parseschema(src; allow_invalid_names=false, allow_invalid_defaults=false, limits=Limits()) -> Schema

Parse an Avro schema from JSON text (a `String`, a byte vector, or an `IO` read incrementally under
`max_schema_bytes`). Every rule of plan §3/§4.2 is validated; violations raise `SchemaError` (with a
JSON path) or `LimitError`. With `allow_invalid_names=true` invalid names are admitted (the graph is
marked `repaired_names`); with `allow_invalid_defaults=true` invalid defaults are kept with
`valid=false`.
"""
function parseschema(src; allow_invalid_names::Bool=false, allow_invalid_defaults::Bool=false, limits::Limits=Limits(),
                     budget::Union{Nothing,Budget}=nothing)
    return parseschemaimpl(src, false; allow_invalid_names=allow_invalid_names,
        allow_invalid_defaults=allow_invalid_defaults, limits=limits, budget=budget)
end

function parseschemaimpl(src, legacy_fixed_names::Bool; allow_invalid_names::Bool=false,
                         allow_invalid_defaults::Bool=false, limits::Limits=Limits(),
                         budget::Union{Nothing,Budget}=nothing)
    budget === nothing &&
        return withbudget(b -> parseschemaimpl(src, legacy_fixed_names; allow_invalid_names=allow_invalid_names,
                                               allow_invalid_defaults=allow_invalid_defaults,
                                               limits=limits, budget=b), limits)
    buf = sourcebytes(src, limits.max_schema_bytes, budget, SchemaError)
    errfn = (msg, pos) -> throw(SchemaError(string(msg, " (byte ", pos, ")"), "\$"))
    limitfn = (limit, observed, value) -> throw(LimitError(limit, observed, value, limit, :decode))
    tree = parsejson(buf; maxbytes=limits.max_schema_bytes, maxdepth=limits.max_schema_depth, errfn=errfn, budget=budget,
        limitfn=limitfn, bytelimit=:max_schema_bytes, depthlimit=:max_schema_depth)
    ctx = ParseContext(limits, budget, allow_invalid_names, allow_invalid_defaults, legacy_fixed_names)
    s = parsenode(ctx, tree, "", "\$", buf)
    isempty(ctx.pending) || schemaerror("internal error: unfilled record", "\$")
    return finalize!(ctx, s)
end

"""
    sourcebytes(src, maxbytes, budget, E) -> Vector{UInt8} or String codeunits view

Normalise a source to contiguous bytes: `String`/`SubString{String}`/`Vector{UInt8}`/unit-stride views
are used in place; other strings and vectors are copied (charged); an `IO` is read incrementally and
stops after `maxbytes + 1` bytes.
"""
function sourcebytes(src::Union{String,SubString{String}}, maxbytes::Int, budget::Budget, ::Type{E}) where {E}
    return codeunits(src)
end

function sourcebytes(src::Vector{UInt8}, maxbytes::Int, budget::Budget, ::Type{E}) where {E}
    return src
end

function sourcebytes(src::AbstractString, maxbytes::Int, budget::Budget, ::Type{E}) where {E}
    n = sizeof(src) + 40
    reserve!(budget, n)
    out = Vector{UInt8}(codeunits(String(src)))
    allocated!(budget, n)
    return out
end
function sourcebytes(src::AbstractVector{UInt8}, maxbytes::Int, budget::Budget, ::Type{E}) where {E}
    if src isa SubArray && parent(src) isa Vector{UInt8} && Base.iscontiguous(src)
        return src
    end
    n = length(src) + 40
    reserve!(budget, n)
    out = Vector{UInt8}(src)
    allocated!(budget, n)
    return out
end
function sourcebytes(io::IO, maxbytes::Int, budget::Budget, ::Type{E}) where {E}
    readlimit = maxbytes == typemax(Int) ? typemax(Int) : maxbytes + 1
    cap = min(64 * KiB, readlimit)
    reserve!(budget, bytesbytes(cap))
    out = Vector{UInt8}(undef, cap)
    allocated!(budget, bytesbytes(cap))
    chunkcap = min(64 * KiB, readlimit)
    reserve!(budget, bytesbytes(chunkcap))
    chunk = Vector{UInt8}(undef, chunkcap)
    allocated!(budget, bytesbytes(chunkcap))
    len = 0
    while !eof(io)
        n = readbytes!(io, chunk, min(length(chunk), readlimit - len))
        n == 0 && break
        need = checked_add(len, n)
        if need > cap
            grown = cap > typemax(Int) - cap ? typemax(Int) : cap + cap
            newcap = min(max(grown, need), readlimit)
            reserve!(budget, bytesbytes(newcap))
            replacement = Vector{UInt8}(undef, newcap)
            allocated!(budget, bytesbytes(newcap))
            copyto!(replacement, 1, out, 1, len)
            out = replacement                          # the old buffer is unreachable only after the rebind
            release!(budget, bytesbytes(cap))
            cap = newcap
        end
        copyto!(out, len + 1, chunk, 1, n)
        len = need
        if len > maxbytes
            limit = E === SchemaError ? :max_schema_bytes : :max_datum_bytes
            throw(LimitError(limit, len, maxbytes, limit, :decode))
        end
    end
    release!(budget, bytesbytes(chunkcap))
    if len != cap
        reserve!(budget, bytesbytes(len))
        exact = Vector{UInt8}(undef, len)
        allocated!(budget, bytesbytes(len))
        copyto!(exact, 1, out, 1, len)
        out = exact                                    # the old buffer is unreachable only after the rebind
        release!(budget, bytesbytes(cap))
    end
    return out
end

function parsenode(ctx::ParseContext, node, enclosing::String, path::String, buf)
    ctx.depth += 1
    ctx.depth <= ctx.limits.max_schema_depth || throw(LimitError(:max_schema_depth, ctx.depth, ctx.limits.max_schema_depth, :max_schema_depth, :decode))
    try
        if node isa String
            return parsereference(ctx, node, enclosing, path)
        elseif node isa JSONArray
            return parseunion(ctx, node, enclosing, path, buf)
        elseif node isa JSONObject
            return parseobjectschema(ctx, node, enclosing, path, buf)
        else
            schemaerror("a schema must be a JSON string, object or array", path)
        end
    finally
        ctx.depth -= 1
    end
end

function parsereference(ctx::ParseContext, name::String, enclosing::String, path::String)
    if name in PRIMITIVE_NAMES
        return primitive(ctx, name, EMPTY_PROPS, path)
    end
    checknamebytes(ctx, name, "name", path)
    full = resolvereference(name, enclosing)
    s = get(ctx.named, full, nothing)
    s === nothing && !isempty(enclosing) && (s = get(ctx.named, name, nothing))   # spec: a bare reference may name a null-namespace type
    s === nothing && schemaerror("undefined type \"$(escapename(name))\" (types must be defined before use)", path)
    return s
end

function primitive(ctx::ParseContext, name::String, p::Props, path::String)
    meta = newmeta!(ctx, path)
    name == "null" && return settlednode(ctx, NullSchema(p, meta))
    name == "boolean" && return settlednode(ctx, BooleanSchema(p, meta))
    name == "int" && return settlednode(ctx, IntSchema(evaluatelogical(:int, 0, p), p, meta))
    name == "long" && return settlednode(ctx, LongSchema(evaluatelogical(:long, 0, p), p, meta))
    name == "float" && return settlednode(ctx, FloatSchema(p, meta))
    name == "double" && return settlednode(ctx, DoubleSchema(p, meta))
    name == "bytes" && return settlednode(ctx, BytesSchema(evaluatelogical(:bytes, 0, p), p, meta))
    return settlednode(ctx, StringSchema(evaluatelogical(:string, 0, p), p, meta))
end

function parseunion(ctx::ParseContext, arr::JSONArray, enclosing::String, path::String, buf)
    length(arr) <= ctx.limits.max_union_branches ||
        throw(LimitError(:max_union_branches, length(arr), ctx.limits.max_union_branches, :max_union_branches, :decode))
    branchbytes = vectorbytes(Schema, length(arr)) + 24
    reserve!(ctx.budget, branchbytes)              # the exact branch capacity and frozen shell (§4.4)
    branches = emptywithcapacity(FrozenVector{Schema}, length(arr))
    allocated!(ctx.budget, branchbytes)
    for (i, b) in enumerate(arr)
        bpath = string(path, "[", i - 1, "]")
        b isa JSONArray && schemaerror("unions may not immediately contain other unions", bpath)
        s = parsenode(ctx, b, enclosing, bpath, buf)
        s isa UnionSchema && schemaerror("unions may not immediately contain other unions", bpath)
        ident = branchidentity(s)
        for prev in branches
            branchidentity(prev) == ident && schemaerror("duplicate union branch $(ident[6:end])", bpath)
        end
        push!(branches, s)
    end
    return settlednode(ctx, UnionSchema(freeze!(branches), newmeta!(ctx, path)))
end

function branchidentity(s::NamedSchema)
    return string("name:", fullname(s))
end

function branchidentity(s::Schema)
    return string("kind:", kind(s))
end

function parseobjectschema(ctx::ParseContext, obj::JSONObject, enclosing::String, path::String, buf)
    haskey(obj, "type") || schemaerror("schema object without a \"type\" attribute", path)
    t = obj["type"]
    t isa String || schemaerror("a schema object's \"type\" must be a JSON string naming a primitive or one of record, error, enum, array, map, fixed", string(path, ".type"))
    if t in PRIMITIVE_NAMES
        p = collectprops(ctx, obj, ("type",), path)
        return primitive(ctx, t, p, path)
    elseif t == "array"
        haskey(obj, "items") || schemaerror("array schema without \"items\"", path)
        items = parsenode(ctx, obj["items"], enclosing, string(path, ".items"), buf)
        p = collectprops(ctx, obj, SCHEMA_GRAMMAR[:array], path)
        return settlednode(ctx, ArraySchema(items, p, newmeta!(ctx, path)))
    elseif t == "map"
        haskey(obj, "values") || schemaerror("map schema without \"values\"", path)
        values = parsenode(ctx, obj["values"], enclosing, string(path, ".values"), buf)
        p = collectprops(ctx, obj, SCHEMA_GRAMMAR[:map], path)
        return settlednode(ctx, MapSchema(values, p, newmeta!(ctx, path)))
    elseif t == "record" || t == "error"
        return parserecord(ctx, obj, enclosing, path, buf, t == "error")
    elseif t == "enum"
        return parseenum(ctx, obj, enclosing, path, buf)
    elseif t == "fixed"
        return parsefixed(ctx, obj, enclosing, path)
    else
        schemaerror("a schema object's \"type\" must name a primitive or one of record, error, enum, array, map, fixed (a named-type reference is a schema string, a union is an array); got \"$(escapename(t))\"", string(path, ".type"))
    end
end

function parsenamed(ctx::ParseContext, obj::JSONObject, enclosing::String, path::String)
    name = stringattr(obj, "name", path; required=true)
    namespace = haskey(obj, "namespace") ? obj["namespace"] : nothing
    namespace === nothing || namespace isa String || schemaerror("attribute \"namespace\" must be a JSON string", string(path, ".namespace"))
    full = resolvefullname(name, namespace, enclosing)
    checkname(ctx, full.name, "name", string(path, ".name"))
    checknamespace(ctx, full.namespace, string(path, ".namespace"))
    isreservedfullname(full) && schemaerror("\"$(full.name)\" is a primitive type name and cannot be redefined in the null namespace", string(path, ".name"))
    haskey(ctx.named, fullname(full)) && schemaerror("duplicate type name \"$(fullname(full))\"", string(path, ".name"))
    ctx.namedcount[] += 1
    ctx.namedcount[] <= ctx.limits.max_named_types ||
        throw(LimitError(:max_named_types, ctx.namedcount[], ctx.limits.max_named_types, :max_named_types, :decode))
    raw = stringarrayattr(ctx, obj, "aliases", path)
    if raw === nothing || isempty(raw)
        return full, EMPTY_STRING_LIST, EMPTY_STRING_LIST
    end
    aliases = BuildBuf{String}(ctx.budget, length(raw))
    for (i, a) in enumerate(raw)
        checknamebytes(ctx, a, "alias", string(path, ".aliases[", i - 1, "]"))
        na = normalizealias(a, full.namespace)
        na == fullname(full) && continue                       # self-alias: idempotent, ignored
        found = false
        for j in 1:aliases.len
            aliases.data[j] == na && (found = true; break)
        end
        found && continue
        retain!(ctx.budget, sizeof(na))                        # the normalised copy the node keeps
        push!(aliases, ctx.budget, na)
    end
    reserve!(ctx.budget, 48)                                   # the two frozen wrappers
    wrapped = (full, freeze!(FrozenVector{String}(finishbuild!(aliases, ctx.budget), false)),
               freeze!(FrozenVector{String}(raw, false)))
    allocated!(ctx.budget, 48)
    return wrapped
end

function register!(ctx::ParseContext, s::NamedSchema, path::String)
    full = fullname(s)
    for (existing, other) in ctx.named
        other isa NamedSchema || continue
        if full in other.aliases || any(a -> a == existing || a in other.aliases, s.aliases)
            schemaerror("alias collision between \"$full\" and \"$existing\"", path)
        end
    end
    budgetedinsert!(ctx.named, full, s, ctx.budget)
    retain!(ctx.budget, sizeof(full) + 16)         # the retained fullname key and its table slot
    for a in s.aliases
        retain!(ctx.budget, sizeof(a) + 24)        # each alias the table keeps alive
    end
    return s
end

function parsefixed(ctx::ParseContext, obj::JSONObject, enclosing::String, path::String)
    full, aliases, raw = if ctx.legacyfixednames && !haskey(obj, "name")
        # Avro.jl ≤ 1.1.2 wrote nameless fixed schemas (they carry no references, so a synthetic name is unambiguous)
        ctx.legacyfixedcount = checked_add(ctx.legacyfixedcount, 1)
        (FullName(string("_avrojl1_fixed_", ctx.legacyfixedcount), ""), freeze!(FrozenVector{String}()), freeze!(FrozenVector{String}()))
    else
        parsenamed(ctx, obj, enclosing, path)
    end
    haskey(obj, "size") || schemaerror("fixed schema without \"size\"", path)
    sz = obj["size"]
    sz isa Int64 && sz >= 0 || schemaerror("fixed \"size\" must be a non-negative JSON integer", string(path, ".size"))
    p = collectprops(ctx, obj, SCHEMA_GRAMMAR[:fixed], path)
    s = settlednode(ctx, FixedSchema(full, aliases, raw, Int(sz), evaluatelogical(:fixed, Int(sz), p), p, newmeta!(ctx, path)))
    return register!(ctx, s, path)
end

function parseenum(ctx::ParseContext, obj::JSONObject, enclosing::String, path::String, buf)
    full, aliases, raw = parsenamed(ctx, obj, enclosing, path)
    syms = stringarrayattr(ctx, obj, "symbols", path; required=true)
    length(syms) <= ctx.limits.max_enum_symbols ||
        throw(LimitError(:max_enum_symbols, length(syms), ctx.limits.max_enum_symbols, :max_enum_symbols, :decode))
    indexbytes = vectorbytes(String, length(syms)) + vectorbytes(Int, length(syms)) + 32
    reserve!(ctx.budget, indexbytes)               # the exact symbol-index capacity and shell (§4.4)
    index = emptywithcapacity(FrozenDict{String,Int}, length(syms))
    allocated!(ctx.budget, indexbytes)
    for (i, sym) in enumerate(syms)
        spath = string(path, ".symbols[", i - 1, "]")
        checkname(ctx, sym, "enum symbol", spath)
        haskey(index, sym) && schemaerror("duplicate enum symbol \"$(escapename(sym))\"", spath)
        retain!(ctx.budget, sizeof(sym) + 32)      # the symbol string the schema keeps alive
        index[sym] = i
    end
    doc = stringattr(obj, "doc", path)
    default = nodefault
    if haskey(obj, "default")
        d = obj["default"]
        dpath = string(path, ".default")
        if d isa String && haskey(index, d)
            default = DefaultValue(d, 0, spanof(obj, "default", buf), index[d], true)
        else
            ctx.allow_invalid_defaults || schemaerror("enum default must be one of the symbols", dpath)
            ctx.repaired_defaults = true
            default = DefaultValue(d, 0, spanof(obj, "default", buf), 0, false)
        end
    end
    p = collectprops(ctx, obj, SCHEMA_GRAMMAR[:enum], path)
    s = settlednode(ctx, EnumSchema(full, aliases, raw, doc, freeze!(FrozenVector{String}(syms, false)), default, freeze!(index), p, newmeta!(ctx, path)))
    return register!(ctx, s, path)
end

function spanof(obj::JSONObject, key::String, buf)
    isempty(obj.spans) && return ""
    for (i, k) in enumerate(obj.order)
        k == key || continue
        r = obj.spans[i]
        return String(buf[r])
    end
    return ""
end

function parserecord(ctx::ParseContext, obj::JSONObject, enclosing::String, path::String, buf, iserror::Bool)
    full, aliases, raw = parsenamed(ctx, obj, enclosing, path)
    haskey(obj, "fields") || schemaerror("record schema without \"fields\"", path)
    farr = obj["fields"]
    farr isa JSONArray || schemaerror("\"fields\" must be a JSON array", string(path, ".fields"))
    length(farr) <= ctx.limits.max_fields || throw(LimitError(:max_fields, length(farr), ctx.limits.max_fields, :max_fields, :decode))
    doc = stringattr(obj, "doc", path)
    p = collectprops(ctx, obj, SCHEMA_GRAMMAR[:record], path)
    nfields = length(farr)
    fieldstate = vectorbytes(Field, nfields) + vectorbytes(String, nfields) + vectorbytes(Int, nfields) + 56
    reserve!(ctx.budget, fieldstate)               # exact field-vector and index capacity plus shells (§4.4)
    fields = emptywithcapacity(FrozenVector{Field}, nfields)
    fieldindex = emptywithcapacity(FrozenDict{String,Int}, nfields)
    allocated!(ctx.budget, fieldstate)
    rec = settlednode(ctx, RecordSchema(full, aliases, raw, doc, iserror, p, fields, fieldindex, newmeta!(ctx, path)))
    register!(ctx, rec, path)                      # register before filling so self-references resolve
    push!(ctx.pending, rec)
    ns = full.namespace
    for (i, f) in enumerate(farr)
        fpath = string(path, ".fields[", i - 1, "]")
        f isa JSONObject || schemaerror("each field must be a JSON object", fpath)
        field = parsefield(ctx, f, ns, fpath, buf)
        haskey(fieldindex, field.name) && schemaerror("duplicate field name \"$(escapename(field.name))\"", string(fpath, ".name"))
        for a in field.aliases
            (a == field.name || haskey(fieldindex, a)) && continue
        end
        retain!(ctx.budget, sizeof(field.name) + 96)   # the field name, Field shell and index entry kept alive
        push!(fields, field)
        fieldindex[field.name] = i
    end
    # field alias collisions with other field names / aliases
    for (i, fa) in enumerate(fields), a in fa.aliases
        a == fa.name && continue
        haskey(fieldindex, a) && schemaerror("field alias \"$(escapename(a))\" collides with a field name", string(path, ".fields[", i - 1, "].aliases"))
        for (j, fb) in enumerate(fields)
            j == i && continue
            a in fb.aliases && schemaerror("field alias \"$(escapename(a))\" is declared by two fields", string(path, ".fields[", i - 1, "].aliases"))
        end
    end
    freeze!(fields)
    freeze!(fieldindex)
    pop!(ctx.pending)
    return rec
end

function parsefield(ctx::ParseContext, obj::JSONObject, ns::String, path::String, buf)
    name = stringattr(obj, "name", path; required=true)
    checkname(ctx, name, "field name", string(path, ".name"))
    haskey(obj, "type") || schemaerror("field without \"type\"", path)
    schema = parsenode(ctx, obj["type"], ns, string(path, ".type"), buf)
    doc = stringattr(obj, "doc", path)
    order = :ascending
    if haskey(obj, "order")
        o = obj["order"]
        o isa String && o in ("ascending", "descending", "ignore") || schemaerror("\"order\" must be \"ascending\", \"descending\" or \"ignore\"", string(path, ".order"))
        order = Symbol(o)
    end
    rawaliases = stringarrayattr(ctx, obj, "aliases", path)
    if rawaliases === nothing || isempty(rawaliases)
        rawaliases = String[]
    end
    aliases = BuildBuf{String}(ctx.budget, length(rawaliases))
    for (i, a) in enumerate(rawaliases)
        checknamebytes(ctx, a, "field alias", string(path, ".aliases[", i - 1, "]"))
        a == name && continue
        found = false
        for j in 1:aliases.len
            aliases.data[j] == a && (found = true; break)
        end
        found || push!(aliases, ctx.budget, a)
    end
    default = nodefault
    if haskey(obj, "default")
        default = makedefault(ctx, schema, obj["default"], spanof(obj, "default", buf), string(path, ".default"))
    end
    p = collectprops(ctx, obj, FIELD_GRAMMAR, path)
    if aliases.len == 0
        return Field(name, schema, doc, default, order, EMPTY_STRING_LIST, p)
    end
    reserve!(ctx.budget, 24)                                   # the frozen wrapper (the Field shell is retained by its record)
    f = Field(name, schema, doc, default, order, freeze!(FrozenVector{String}(finishbuild!(aliases, ctx.budget), false)), p)
    allocated!(ctx.budget, 24)
    return f
end

function makedefault(ctx::ParseContext, schema::Schema, json, span::String, path::String)
    retain!(ctx.budget, sizeof(span))              # the retained source-span copy
    ok, branch = validatedefault(schema, json, ctx.limits.max_depth)
    reserve!(ctx.budget, 64)
    d = ok ? DefaultValue(json, branch, span, 0, true) : begin
        ctx.allow_invalid_defaults || schemaerror("default value does not match the field's schema", path)
        ctx.repaired_defaults = true
        DefaultValue(json, 0, span, 0, false)
    end
    allocated!(ctx.budget, 64)
    return d
end

# ---- the recursive default rule (plan §4.2) ---------------------------------------------------------

"""
    validatedefault(schema, json, maxdepth) -> (ok::Bool, branch::Int)

Validate a JSON default against a schema: union defaults are the bare JSON of the value matched
against the branches in declaration order (returning the 1-based branch); record defaults may omit
fields that have their own defaults and ignore unknown members; every other kind follows the strict
datum-JSON row of plan §4.11.
"""
function validatedefault(schema::Schema, json, maxdepth::Int, depth::Int=1)
    depth <= maxdepth && return (defaultmatches(schema, json, maxdepth, depth), 0)
    return (false, 0)
end

function validatedefault(schema::UnionSchema, json, maxdepth::Int, depth::Int=1)
    depth <= maxdepth || return (false, 0)
    for (i, b) in enumerate(schema.branches)
        defaultmatches(b, json, maxdepth, depth + 1) && return (true, i)
    end
    return (false, 0)
end

function defaultmatches(::NullSchema, json, maxdepth, depth)
    return json === nothing
end

function defaultmatches(::BooleanSchema, json, maxdepth, depth)
    return json isa Bool
end

function defaultmatches(::IntSchema, json, maxdepth, depth)
    return json isa Int64 && typemin(Int32) <= json <= typemax(Int32)
end

function defaultmatches(::LongSchema, json, maxdepth, depth)
    return json isa Int64
end

function defaultmatches(::Union{FloatSchema,DoubleSchema}, json, maxdepth, depth)
    return json isa Int64 || json isa Float64 || json isa JSONNumber || (json isa String && json in ("NaN", "Infinity", "-Infinity"))
end

function defaultmatches(::BytesSchema, json, maxdepth, depth)
    return json isa String && isbytestring(json)
end

function defaultmatches(s::FixedSchema, json, maxdepth, depth)
    return json isa String && isbytestring(json) && bytestringlength(json) == s.size
end

function defaultmatches(::StringSchema, json, maxdepth, depth)
    return json isa String && isstrictutf8(json)
end

function defaultmatches(s::EnumSchema, json, maxdepth, depth)
    return json isa String && haskey(s.symbolindex, json)
end

function defaultmatches(s::ArraySchema, json, maxdepth, depth)
    json isa JSONArray || return false
    depth < maxdepth || return false
    for x in json
        validatedefault(s.items, x, maxdepth, depth + 1)[1] || return false
    end
    return true
end
function defaultmatches(s::MapSchema, json, maxdepth, depth)
    json isa JSONObject || return false
    depth < maxdepth || return false
    for (k, v) in json.members
        isstrictutf8(k) || return false
        validatedefault(s.values, v, maxdepth, depth + 1)[1] || return false
    end
    return true
end
function defaultmatches(s::RecordSchema, json, maxdepth, depth)
    json isa JSONObject || return false
    depth < maxdepth || return false
    for f in s.fields
        if haskey(json, f.name)
            validatedefault(f.schema, json[f.name], maxdepth, depth + 1)[1] || return false
        else
            f.default isa DefaultValue && f.default.valid || return false
        end
    end
    return true       # unknown members are ignored (the span is retained verbatim)
end
function defaultmatches(s::UnionSchema, json, maxdepth, depth)
    return validatedefault(s, json, maxdepth, depth)[1]
end

"""
    isbytestring(s) -> Bool

`true` when every code point of `s` is ≤ U+00FF (the JSON form of `bytes`/`fixed`).
"""
function isbytestring(s::AbstractString)
    isstrictutf8(s) || return false
    for c in s
        UInt32(c) <= 0xFF || return false
    end
    return true
end

function bytestringlength(s::AbstractString)
    return length(s)
end

# ---- finalisation: ids, graph info, hashes -----------------------------------------------------------

function finalize!(ctx::ParseContext, root::Schema)
    info = GraphInfo(ctx.limits, ctx.repaired_names, ctx.repaired_defaults, length(ctx.metas), ctx.namedcount[])
    for (i, m) in enumerate(ctx.metas)
        fillonce!(m.id, Int32(i - 1))
        fillonce!(m.graph, info)
    end
    computehashes!(root, ctx.metas)
    return root
end

function computehashes!(root::Schema, metas::Vector{NodeMeta})
    inprogress = falses(length(metas))
    schemahash(root, inprogress)
    return root
end

function schemahash(s::Schema, inprogress::BitVector)
    isfilled(s.meta.hash) && return s.meta.hash[]
    id = Int(s.meta.id[]) + 1
    if inprogress[id]
        return s isa NamedSchema ? hash(fullname(s), UInt(0x5ecc1e)) : UInt(0x1)
    end
    inprogress[id] = true
    h = structuralhash(s, inprogress)
    inprogress[id] = false
    isfilled(s.meta.hash) || fillonce!(s.meta.hash, UInt64(h))
    return UInt64(h)
end

function propshash(p::Props, h::UInt)
    return hash(p.vals, hash(p.keys, h))
end

function logicalhash(::Nothing, h::UInt)
    return h
end

function logicalhash(l::DecimalLogical, h::UInt)
    return hash(l.scale, hash(l.precision, hash(:decimal, h)))
end

function logicalhash(l::LogicalType, h::UInt)
    return hash(logicalname(l), h)
end

function structuralhash(s::PrimitiveSchema, inprogress)
    return propshash(s.props, logicalhash(logical(s), hash(kind(s), UInt(0xa7))))
end

function structuralhash(s::ArraySchema, inprogress)
    return propshash(s.props, hash(schemahash(s.items, inprogress), hash(:array, UInt(0xa7))))
end

function structuralhash(s::MapSchema, inprogress)
    return propshash(s.props, hash(schemahash(s.values, inprogress), hash(:map, UInt(0xa7))))
end

function structuralhash(s::UnionSchema, inprogress)
    h = hash(:union, UInt(0xa7))
    for b in s.branches
        h = hash(schemahash(b, inprogress), h)
    end
    return h
end
function structuralhash(s::FixedSchema, inprogress)
    h = hash(fullname(s), hash(:fixed, UInt(0xa7)))
    h = hash(s.aliases.data, hash(s.size, h))
    return propshash(s.props, logicalhash(s.logical, h))
end

function defaulthash(::NoDefault, h::UInt)
    return hash(:nodefault, h)
end

function defaulthash(d::DefaultValue, h::UInt)
    return d.valid ? hash(d.branch, hash(d.json, h)) : hash(d.span, hash(:invalid, h))
end

function structuralhash(s::EnumSchema, inprogress)
    h = hash(fullname(s), hash(:enum, UInt(0xa7)))
    h = hash(s.symbols.data, hash(s.aliases.data, hash(s.doc, h)))
    return propshash(s.props, defaulthash(s.default, h))
end
function structuralhash(s::RecordSchema, inprogress)
    h = hash(fullname(s), hash(s.iserror ? :error : :record, UInt(0xa7)))
    h = hash(s.aliases.data, hash(s.doc, h))
    for f in s.fields
        h = hash(f.name, h)
        h = hash(schemahash(f.schema, inprogress), h)
        h = defaulthash(f.default, h)
        h = hash(f.order, hash(f.aliases.data, h))
        h = propshash(f.props, hash(f.doc, h))
    end
    return propshash(s.props, h)
end

# ---- structural equality --------------------------------------------------------------------------

"""
    ==(a::Schema, b::Schema)

Structural equality (plan §4.2): kind, fullname, fields (name, schema, default value and selected
branch, order, normalised aliases, doc), symbols, enum default, size, logical type, `iserror`, and
`props` as unordered maps of JSON values; cyclic graphs terminate through a visited-pair set keyed by
dense node ids, charged to `max_resolution_work` of the larger recorded limits.
"""
function Base.:(==)(a::Schema, b::Schema)
    a === b && return true
    typeof(a) === typeof(b) || return false
    limits = larger(graphinfo(a).limits, graphinfo(b).limits)
    budget = Budget(limits; available=typemax(Int) ÷ 4)
    try
        return budgetedschemaequal(a, b, budget)
    finally
        close!(budget)
    end
end

function larger(a::Limits, b::Limits)
    return a.max_resolution_work >= b.max_resolution_work ? a : b
end

"An exact outer table of sorted partner ids used only during one structural comparison."
mutable struct SchemaEqualityMemo
    partners::Union{Nothing,Vector{Vector{Int32}}}
    budget::Budget
    charge::Int
end

function schemaequalitymemo(a::Schema, budget::Budget)
    checkpoint = budgetcheckpoint(budget)
    try
        n = graphinfo(a).nodes
        outercharge = vectorbytes(Vector{Int32}, n)
        reserve!(budget, outercharge)
        partners = Vector{Vector{Int32}}(undef, n)
        allocated!(budget, outercharge)
        total = outercharge
        for i in eachindex(partners)
            innercharge = vectorbytes(Int32, 0)
            reserve!(budget, innercharge)
            partners[i] = Int32[]
            allocated!(budget, innercharge)
            total = checked_add(total, innercharge)
        end
        return SchemaEqualityMemo(partners, budget, total)
    catch
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

function releaseequalitymemo!(memo::SchemaEqualityMemo)
    charge = memo.charge
    memo.partners = nothing
    memo.charge = 0
    release!(memo.budget, charge)
    return nothing
end

function visitpair!(memo::SchemaEqualityMemo, a::Schema, b::Schema, budget::Budget)
    visited = memo.partners::Vector{Vector{Int32}}
    ia = Int(nodeid(a)) + 1
    partners = visited[ia]
    ib = nodeid(b)
    i = searchsortedfirst(partners, ib)
    addresolution!(budget, 1 + (i <= length(partners) ? 1 : 0))
    i <= length(partners) && partners[i] == ib && return true
    n = checked_add(length(partners), 1)
    replacementcharge = vectorbytes(Int32, n)
    checkpoint = budgetcheckpoint(budget)
    try
        reserve!(budget, replacementcharge)
        replacement = Vector{Int32}(undef, n)
        allocated!(budget, replacementcharge)
        copyto!(replacement, 1, partners, 1, i - 1)
        replacement[i] = ib
        copyto!(replacement, i + 1, partners, i, n - i)
        visited[ia] = replacement
    catch
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
    oldcharge = vectorbytes(Int32, n - 1)
    release!(budget, oldcharge)
    memo.charge = checked_add(memo.charge, replacementcharge - oldcharge)
    addresolution!(budget, n - i + 1)
    return false
end

function budgetedschemaequal(a::Schema, b::Schema, budget::Budget)
    a === b && return true
    typeof(a) === typeof(b) || return false
    memo = schemaequalitymemo(a, budget)
    try
        return schemaequal(a, b, memo, budget)
    finally
        releaseequalitymemo!(memo)
    end
end

function schemaequal(a::Schema, b::Schema, visited, budget)
    a === b && return true
    typeof(a) === typeof(b) || return false
    visitpair!(visited, a, b, budget) && return true
    return structuralequal(a, b, visited, budget)
end

function propsequal(a::Props, b::Props)
    return a.keys == b.keys && a.vals == b.vals
end

function structuralequal(a::PrimitiveSchema, b::PrimitiveSchema, visited, budget)
    return logical(a) == logical(b) && propsequal(a.props, b.props)
end

function structuralequal(a::ArraySchema, b::ArraySchema, visited, budget)
    return propsequal(a.props, b.props) && schemaequal(a.items, b.items, visited, budget)
end

function structuralequal(a::MapSchema, b::MapSchema, visited, budget)
    return propsequal(a.props, b.props) && schemaequal(a.values, b.values, visited, budget)
end

function structuralequal(a::UnionSchema, b::UnionSchema, visited, budget)
    length(a.branches) == length(b.branches) || return false
    for (x, y) in zip(a.branches, b.branches)
        schemaequal(x, y, visited, budget) || return false
    end
    return true
end

function structuralequal(a::FixedSchema, b::FixedSchema, visited, budget)
    return a.name == b.name && a.size == b.size && a.aliases.data == b.aliases.data && a.logical == b.logical && propsequal(a.props, b.props)
end

function defaultequal(a::NoDefault, b::NoDefault)
    return true
end

function defaultequal(a::DefaultValue, b::DefaultValue)
    return a.valid == b.valid && (a.valid ? (a.branch == b.branch && a.json == b.json) : a.span == b.span)
end

function defaultequal(a::Default, b::Default)
    return false
end

function structuralequal(a::EnumSchema, b::EnumSchema, visited, budget)
    return a.name == b.name && a.symbols.data == b.symbols.data && a.aliases.data == b.aliases.data && a.doc == b.doc &&
        defaultequal(a.default, b.default) && propsequal(a.props, b.props)
end

function structuralequal(a::RecordSchema, b::RecordSchema, visited, budget)
    a.name == b.name && a.iserror == b.iserror && a.aliases.data == b.aliases.data && a.doc == b.doc && propsequal(a.props, b.props) || return false
    length(a.fields) == length(b.fields) || return false
    for (f, g) in zip(a.fields, b.fields)
        f.name == g.name && f.order == g.order && f.aliases.data == g.aliases.data && f.doc == g.doc && propsequal(f.props, g.props) || return false
        defaultequal(f.default, g.default) || return false
        schemaequal(f.schema, g.schema, visited, budget) || return false
    end
    return true
end

function Base.:(==)(a::DecimalLogical, b::DecimalLogical)
    return a.precision == b.precision && a.scale == b.scale
end

function Base.:(==)(a::UnknownLogical, b::UnknownLogical)
    return a.name == b.name
end

# ---- printing ---------------------------------------------------------------------------------------

"""
A charging output sink for the schema printers (plan §4.4, amendment round 1): produced text is the
work-rule input, growth is reserved in chunks before it is written, and the text is bounded by
`max_schema_bytes`. The default limits are the schema's own recorded limits, so printing an admitted
schema always succeeds; callers may pass stricter ones.
"""
mutable struct BoundedWriter <: IO
    buf::Vector{UInt8}      # a package-owned charged buffer grown by reserved exact replacement (D02)
    len::Int
    const budget::Budget
    const maxbytes::Int
end

function BoundedWriter(budget::Budget, maxbytes::Int)
    cap = min(256, maxbytes)
    charge = bytesbytes(cap)
    reserve!(budget, charge)
    try
        buf = Vector{UInt8}(undef, cap)
        allocated!(budget, charge)
        return BoundedWriter(buf, 0, budget, maxbytes)
    catch
        unreserve!(budget, charge)
        rethrow()
    end
end

function boundedgrow!(w::BoundedWriter, n::Int)
    written = checked_add(w.len, n)
    written <= w.maxbytes ||
        throw(LimitError(:max_schema_bytes, written, w.maxbytes, :max_schema_bytes, :encode))
    cap = length(w.buf)
    if written > cap
        grown = cap > w.maxbytes - cap ? w.maxbytes : 2 * cap
        newcap = max(grown, written)
        newcharge = bytesbytes(newcap)
        reserve!(w.budget, newcharge)                  # reserved exact-capacity replacement (§4.4)
        nb = try
            out = Vector{UInt8}(undef, newcap)
            allocated!(w.budget, newcharge)
            out
        catch
            unreserve!(w.budget, newcharge)
            rethrow()
        end
        copyto!(nb, 1, w.buf, 1, w.len)
        w.buf = nb                                     # the old buffer is unreachable only after the rebind
        release!(w.budget, bytesbytes(cap))
    end
    addinput!(w.budget, n)                             # produced text, not reserved capacity, is the denominator
    return nothing
end

function Base.write(w::BoundedWriter, b::UInt8)
    boundedgrow!(w, 1)
    w.len += 1
    @inbounds w.buf[w.len] = b
    return 1
end

function Base.unsafe_write(w::BoundedWriter, p::Ptr{UInt8}, n::UInt)
    boundedgrow!(w, Int(n))
    GC.@preserve w unsafe_copyto!(pointer(w.buf, w.len + 1), p, Int(n))
    w.len += Int(n)
    return n
end

"The finished text: charged as its String before the buffer's charge is released."
function boundedtake!(w::BoundedWriter)
    stringcharge = stringbytes(w.len)
    reserve!(w.budget, stringcharge)
    out = try
        text = unsafe_string(pointer(w.buf), w.len)
        allocated!(w.budget, stringcharge)
        text
    catch
        unreserve!(w.budget, stringcharge)
        rethrow()
    end
    emptycharge = bytesbytes(0)
    reserve!(w.budget, emptycharge)
    emptybuf = try
        buf = UInt8[]
        allocated!(w.budget, emptycharge)
        buf
    catch
        unreserve!(w.budget, emptycharge)
        rethrow()
    end
    oldcharge = bytesbytes(length(w.buf))
    w.buf = emptybuf
    w.len = 0
    release!(w.budget, oldcharge)
    return out
end

"Compact the finished bytes to exact capacity before hashing or comparing them."
function boundedview(w::BoundedWriter)
    length(w.buf) == w.len && return w.buf
    charge = bytesbytes(w.len)
    reserve!(w.budget, charge)
    exact = try
        buf = Vector{UInt8}(undef, w.len)
        allocated!(w.budget, charge)
        buf
    catch
        unreserve!(w.budget, charge)
        rethrow()
    end
    copyto!(exact, 1, w.buf, 1, w.len)
    oldcharge = bytesbytes(length(w.buf))
    w.buf = exact
    release!(w.budget, oldcharge)
    return exact
end

"Allocate the exact node-indexed seen table used by schema printers."
function schemaseen(s::Schema, budget::Budget)
    n = graphinfo(s).nodes
    charge = vectorbytes(Bool, n)
    reserve!(budget, charge)
    seen = try
        values = Vector{Bool}(undef, n)
        allocated!(budget, charge)
        values
    catch
        unreserve!(budget, charge)
        rethrow()
    end
    fill!(seen, false)
    return seen
end

"Release a printer's dead seen table in exact order: its print has finished (round-3 item 1)."
function releaseseen!(budget::Budget, seen::Vector{Bool})
    release!(budget, vectorbytes(Bool, length(seen)))
    return nothing
end

"Write a named schema's escaped fullname without constructing a joined String."
function escapefullname(io::IO, s::NamedSchema)
    print(io, '"')
    if !isempty(s.name.namespace)
        escapejsoncontents(io, s.name.namespace)
        print(io, '.')
    end
    escapejsoncontents(io, s.name.name)
    print(io, '"')
    return nothing
end

function countnode!(::IO)
    return nothing
end

function countnode!(w::BoundedWriter)
    countvalues!(w.budget)
    return nothing
end

"The schema's recorded construction/parse limits (every root stores them in its `GraphInfo`)."
function graphlimits(s::Schema)
    return graphinfo(s).limits
end

"""
    Avro.json(schema; pretty=false) -> String

The schema as spec JSON: the first occurrence of a named type in full, later references by fullname,
the namespace attribute only when it differs from the enclosing one, custom `props` re-emitted (numbers
verbatim, strings re-escaped), and defaults by their exact source text.
"""
function json(s::Schema; pretty::Bool=false, limits::Limits=graphlimits(s))
    return withbudget(limits) do budget
        w = BoundedWriter(budget, limits.max_schema_bytes)
        seen = schemaseen(s, budget)
        printschema(w, s, "", seen, pretty, 0)
        releaseseen!(budget, seen)
        return boundedtake!(w)
    end
end

function indent(io::IO, pretty::Bool, level::Int)
    pretty || return nothing
    print(io, '\n', ' '^(2 * level))
    return nothing
end

function printjson(io::IO, x, pretty::Bool, level::Int)
    if x === nothing
        print(io, "null")
    elseif x isa Bool
        print(io, x ? "true" : "false")
    elseif x isa Int64
        print(io, x)
    elseif x isa Float64
        print(io, isfinite(x) ? repr(x) : (isnan(x) ? "\"NaN\"" : (x > 0 ? "\"Infinity\"" : "\"-Infinity\"")))
    elseif x isa JSONNumber
        print(io, x.text)
    elseif x isa String
        escapejson(io, x)
    elseif x isa JSONArray
        print(io, '[')
        for (i, v) in enumerate(x)
            i > 1 && print(io, ',')
            indent(io, pretty, level + 1)
            printjson(io, v, pretty, level + 1)
        end
        length(x) > 0 && indent(io, pretty, level)
        print(io, ']')
    elseif x isa JSONObject
        print(io, '{')
        for (i, k) in enumerate(x.order)
            i > 1 && print(io, ',')
            indent(io, pretty, level + 1)
            escapejson(io, k)
            print(io, pretty ? ": " : ":")
            printjson(io, x[k], pretty, level + 1)
        end
        length(x) > 0 && indent(io, pretty, level)
        print(io, '}')
    else
        throw(ArgumentError("cannot print $(typeof(x)) as JSON"))
    end
    return nothing
end

function printprops(io::IO, p::Props, pretty::Bool, level::Int, first::Bool)
    for (k, v) in p
        first || print(io, ',')
        first = false
        indent(io, pretty, level + 1)
        escapejson(io, k)
        print(io, pretty ? ": " : ":")
        printjson(io, v, pretty, level + 1)
    end
    return first
end

function printschema(io::IO, s::Schema, enclosing::String, seen::Vector{Bool}, pretty::Bool, level::Int)
    if s isa PrimitiveSchema
        if isempty(s.props)
            print(io, '"', kind(s), '"')
        else
            print(io, "{")
            indent(io, pretty, level + 1)
            print(io, pretty ? "\"type\": " : "\"type\":", '"', kind(s), '"')
            printprops(io, s.props, pretty, level, false)
            indent(io, pretty, level)
            print(io, '}')
        end
    elseif s isa UnionSchema
        print(io, '[')
        for (i, b) in enumerate(s.branches)
            i > 1 && print(io, ',')
            indent(io, pretty, level + 1)
            printschema(io, b, enclosing, seen, pretty, level + 1)
        end
        length(s.branches) > 0 && indent(io, pretty, level)
        print(io, ']')
    elseif s isa ArraySchema || s isa MapSchema
        print(io, '{')
        indent(io, pretty, level + 1)
        print(io, pretty ? "\"type\": " : "\"type\":", s isa ArraySchema ? "\"array\"," : "\"map\",")
        indent(io, pretty, level + 1)
        print(io, s isa ArraySchema ? (pretty ? "\"items\": " : "\"items\":") : (pretty ? "\"values\": " : "\"values\":"))
        printschema(io, s isa ArraySchema ? s.items : s.values, enclosing, seen, pretty, level + 1)
        printprops(io, s.props, pretty, level, false)
        indent(io, pretty, level)
        print(io, '}')
    else
        seenindex = Int(nodeid(s)) + 1
        if seen[seenindex]
            if s.name.namespace == enclosing
                escapejson(io, s.name.name)
            else
                escapefullname(io, s)
            end
            countnode!(io)
            return nothing
        end
        seen[seenindex] = true
        printnamed(io, s, enclosing, seen, pretty, level)
    end
    countnode!(io)
    return nothing
end

function printnamedheader(io::IO, s::NamedSchema, enclosing::String, pretty::Bool, level::Int)
    print(io, '{')
    indent(io, pretty, level + 1)
    print(io, pretty ? "\"type\": " : "\"type\":", '"', kind(s), "\",")
    indent(io, pretty, level + 1)
    print(io, pretty ? "\"name\": " : "\"name\":")
    escapejson(io, s.name.name)
    if s.name.namespace != enclosing
        print(io, ',')
        indent(io, pretty, level + 1)
        print(io, pretty ? "\"namespace\": " : "\"namespace\":")
        escapejson(io, s.name.namespace)
    end
    if !isempty(s.rawaliases)
        print(io, ',')
        indent(io, pretty, level + 1)
        print(io, pretty ? "\"aliases\": [" : "\"aliases\":[")
        for (i, a) in enumerate(s.rawaliases)
            i > 1 && print(io, ',')
            escapejson(io, a)
        end
        print(io, ']')
    end
    if s isa Union{RecordSchema,EnumSchema} && s.doc !== nothing
        print(io, ',')
        indent(io, pretty, level + 1)
        print(io, pretty ? "\"doc\": " : "\"doc\":")
        escapejson(io, s.doc)
    end
    return nothing
end

function printnamed(io::IO, s::FixedSchema, enclosing::String, seen, pretty::Bool, level::Int)
    printnamedheader(io, s, enclosing, pretty, level)
    print(io, ',')
    indent(io, pretty, level + 1)
    print(io, pretty ? "\"size\": " : "\"size\":", s.size)
    printprops(io, s.props, pretty, level, false)
    indent(io, pretty, level)
    print(io, '}')
    return nothing
end

function printnamed(io::IO, s::EnumSchema, enclosing::String, seen, pretty::Bool, level::Int)
    printnamedheader(io, s, enclosing, pretty, level)
    print(io, ',')
    indent(io, pretty, level + 1)
    print(io, pretty ? "\"symbols\": [" : "\"symbols\":[")
    for (i, sym) in enumerate(s.symbols)
        i > 1 && print(io, ',')
        escapejson(io, sym)
    end
    print(io, ']')
    if s.default isa DefaultValue
        print(io, ',')
        indent(io, pretty, level + 1)
        print(io, pretty ? "\"default\": " : "\"default\":")
        printdefault(io, s.default)
    end
    printprops(io, s.props, pretty, level, false)
    indent(io, pretty, level)
    print(io, '}')
    return nothing
end

function printdefault(io::IO, d::DefaultValue)
    return isempty(d.span) ? printjson(io, d.json, false, 0) : print(io, d.span)
end

function printnamed(io::IO, s::RecordSchema, enclosing::String, seen, pretty::Bool, level::Int)
    printnamedheader(io, s, enclosing, pretty, level)
    print(io, ',')
    indent(io, pretty, level + 1)
    print(io, pretty ? "\"fields\": [" : "\"fields\":[")
    ns = s.name.namespace
    for (i, f) in enumerate(s.fields)
        i > 1 && print(io, ',')
        indent(io, pretty, level + 2)
        print(io, '{')
        indent(io, pretty, level + 3)
        print(io, pretty ? "\"name\": " : "\"name\":")
        escapejson(io, f.name)
        print(io, ',')
        indent(io, pretty, level + 3)
        print(io, pretty ? "\"type\": " : "\"type\":")
        printschema(io, f.schema, ns, seen, pretty, level + 3)
        if f.doc !== nothing
            print(io, ',')
            indent(io, pretty, level + 3)
            print(io, pretty ? "\"doc\": " : "\"doc\":")
            escapejson(io, f.doc)
        end
        if f.default isa DefaultValue
            print(io, ',')
            indent(io, pretty, level + 3)
            print(io, pretty ? "\"default\": " : "\"default\":")
            printdefault(io, f.default)
        end
        if f.order != :ascending
            print(io, ',')
            indent(io, pretty, level + 3)
            print(io, pretty ? "\"order\": " : "\"order\":", '"', f.order, '"')
        end
        if !isempty(f.aliases)
            print(io, ',')
            indent(io, pretty, level + 3)
            print(io, pretty ? "\"aliases\": [" : "\"aliases\":[")
            for (j, a) in enumerate(f.aliases)
                j > 1 && print(io, ',')
                escapejson(io, a)
            end
            print(io, ']')
        end
        printprops(io, f.props, pretty, level + 2, false)
        indent(io, pretty, level + 2)
        print(io, '}')
    end
    length(s.fields) > 0 && indent(io, pretty, level + 1)
    print(io, ']')
    printprops(io, s.props, pretty, level, false)
    indent(io, pretty, level)
    print(io, '}')
    return nothing
end

function Base.show(io::IO, s::Schema)
    return print(io, "Avro.Schema(", json(s), ")")
end

function Base.show(io::IO, ::MIME"text/plain", s::Schema)
    print(io, "Avro.Schema ", json(s; pretty=true))
    return nothing
end

# ---- public constructors ---------------------------------------------------------------------------

function build(::Type{T}, propsin; logical=nothing, limits::Limits=Limits()) where {T<:PrimitiveSchema}
    return withconstruction(limits) do _                # the budget opens before any copy (D01)
        build_(T, propsin; logical=logical, limits=limits)
    end
end

function build_(::Type{T}, propsin; logical=nothing, limits::Limits=Limits()) where {T<:PrimitiveSchema}
    haslogical = T === IntSchema || T === LongSchema || T === BytesSchema || T === StringSchema
    p = makeprops(propsin, ("type",), haslogical ? logical : nothing)
    meta = NodeMeta()
    if haslogical
        k = T === IntSchema ? :int : T === LongSchema ? :long : T === BytesSchema ? :bytes : :string
        s = T(evaluatelogical(k, 0, p), p, meta)
        return finalizepublic!(s, limits, 1, 0)
    end
    s = T(p, meta)
    return finalizepublic!(s, limits, 1, 0)
end

function makeprops(propsin, structural, logical)
    return makeprops(propsin, structural, logical, constructionbudget())
end

# The frozen-dictionary shell (struct plus two empty backing vectors) and one key/value slot pair.
function frozendictshell()
    return 2 * STORAGE[].vector + 48
end

"The charged storage of a frozen dictionary at its current exact replacement capacity."
function frozendictbytes(d::FrozenDict{K,V}) where {K,V}
    return 48 + vectorbytes(K, d.cap) + vectorbytes(V, d.cap)
end

function frozenvectorshell()
    return STORAGE[].vector + 24
end

function makeprops(propsin, structural, logical, budget::Budget)
    sized = Base.IteratorSize(propsin) isa Union{Base.HasLength,Base.HasShape}
    extra = logical === nothing ? 0 : (logical isa DecimalLogical ? 3 : 1)
    cap = (sized ? length(propsin) : 0) + extra
    sized && cap == 0 && return EMPTY_PROPS
    slots = frozendictshell() + 16 * cap
    reserve!(budget, slots)                            # the shell and exact slot capacity (§4.4)
    p = emptywithcapacity(Props, cap)
    allocated!(budget, slots)
    kvs = propsin isa NamedTuple ? pairs(propsin) : propsin
    for (k, v) in kvs
        k isa Union{AbstractString,Symbol} || throw(ArgumentError("`props` keys must be strings or symbols"))
        kn = stringbytes(sizeof(k))
        reserve!(budget, kn)                           # the retained key copy, made next
        ks = String(k)
        allocated!(budget, kn)
        ks in structural && throw(ArgumentError("`props` key \"$ks\" collides with a structural attribute emitted by this constructor"))
        logical !== nothing && ks in ("logicalType", "precision", "scale") &&
            throw(ArgumentError("`props` key \"$ks\" is synthesised by `logical=`"))
        haskey(p, ks) && throw(ArgumentError("duplicate `props` key \"$ks\""))
        budgetedinsert!(p, ks, tojsonvalue(v, budget), budget)   # unsized inputs grow by exact replacement
    end
    if logical !== nothing
        budgetedinsert!(p, "logicalType", logicalname(logical), budget)
        if logical isa DecimalLogical
            bx = 2 * boxbytes(Int64)
            reserve!(budget, bx)                       # the two boxed integers, stored next
            budgetedinsert!(p, "precision", Int64(logical.precision), budget)
            budgetedinsert!(p, "scale", Int64(logical.scale), budget)
            allocated!(budget, bx)
        end
    end
    return freeze!(p)
end

function tojsonvalue(x::Union{Nothing,Bool,Int64,String,JSONNumber,JSONArray,JSONObject}, ::Budget)
    return x
end

function tojsonvalue(x::Integer, ::Budget)
    return Int64(x)
end

function tojsonvalue(x::AbstractFloat, b::Budget)
    isfinite(x) || return isnan(x) ? "NaN" : (x > 0 ? "Infinity" : "-Infinity")
    reserve!(b, 32)
    t = JSONNumber(repr(Float64(x)))
    allocated!(b, 32)
    return t
end

function tojsonvalue(x::AbstractString, b::Budget)
    n = stringbytes(sizeof(x))
    reserve!(b, n)
    t = String(x)
    allocated!(b, n)
    return t
end

function tojsonvalue(x::Symbol, b::Budget)
    n = stringbytes(sizeof(String(x)))
    reserve!(b, n)
    t = String(x)
    allocated!(b, n)
    return t
end

function tojsonvalue(x::AbstractVector, b::Budget)
    n = length(x)
    slots = frozenvectorshell() + vectorbytes(Any, n) + 16
    reserve!(b, slots)                                 # shell, exact capacity and wrapper (§4.4)
    v = emptywithcapacity(FrozenVector{Any}, n)
    allocated!(b, slots)
    for e in x
        push!(v, tojsonvalue(e, b))
    end
    return JSONArray(freeze!(v))
end
function tojsonvalue(x::Union{AbstractDict,NamedTuple}, b::Budget)
    n = length(x)
    slots = frozendictshell() + frozenvectorshell() + 24 * n + 64
    reserve!(b, slots)                                 # shells, exact slot capacity and wrapper (§4.4)
    m = emptywithcapacity(FrozenDict{String,Any}, n)
    order = emptywithcapacity(FrozenVector{String}, n)
    allocated!(b, slots)
    for (k, v) in (x isa NamedTuple ? pairs(x) : x)
        kn = stringbytes(sizeof(k))
        reserve!(b, kn)                                # the retained key copy, made next
        ks = String(k)
        allocated!(b, kn)
        haskey(m, ks) && throw(ArgumentError("duplicate key \"$ks\""))
        m[ks] = tojsonvalue(v, b)
        push!(order, ks)
    end
    return JSONObject(freeze!(m), freeze!(order))
end

# Nested public constructors inside a recursive builder must not finalise the graph early (the record
# under construction is still unfilled); the outermost builder finalises everything. Every outermost
# constructor call owns one import memo, so the same finalised child passed twice is one definition
# plus references.
function builderdepth()
    return get(task_local_storage(), :avro_builder_depth, 0)::Int
end

function importmemo()
    return get(task_local_storage(), :avro_import_memo, nothing)
end

"""
One construction budget per outermost public constructor call (plan §4.4, round-2 D01): the first
public entry opens it and stores it task-locally; nested constructor calls — the builder form
included — charge the same scope, so every copy a construction makes is reserved against one budget.
"""
function withconstruction(f, limits::Limits; direction::Symbol=:decode)
    existing = get(task_local_storage(), :avro_construction_budget, nothing)
    existing === nothing || return f(existing::Budget)
    return withbudget(limits; direction=direction) do b
        task_local_storage(:avro_construction_budget, b)
        try
            return f(b)
        finally
            task_local_storage(:avro_construction_budget, nothing)
        end
    end
end

"The open construction budget of the current public constructor call (an internal invariant)."
function constructionbudget()
    b = get(task_local_storage(), :avro_construction_budget, nothing)
    b === nothing && throw(ArgumentError("internal error: no construction budget is open"))
    return b::Budget
end

function withbuilder(f)
    outer = builderdepth() == 0
    task_local_storage(:avro_builder_depth, builderdepth() + 1)
    memo = nothing
    if outer
        b = constructionbudget()
        reserve!(b, frozendictshell())
        memo = FrozenDict{String,Tuple{Schema,Schema}}()
        allocated!(b, frozendictshell())
        task_local_storage(:avro_import_memo, memo)
    end
    try
        return f()
    finally
        task_local_storage(:avro_builder_depth, builderdepth() - 1)
        if outer
            task_local_storage(:avro_import_memo, nothing)
            release!(constructionbudget(), frozendictbytes(memo::FrozenDict{String,Tuple{Schema,Schema}}))
        end
    end
end

"""
The one construction scope every public constructor, deriver and projection funnels through
(plan §4.4, amendment round 1): the finished graph is walked once under a construction budget that
charges each node and enforces every schema limit the parser enforces — nodes, named types, depth,
per-record fields, union branches, enum symbols, and name/alias bytes — so a caller-supplied
`Limits` bounds construction exactly as it bounds parsing.
"""
function finalizepublic!(s::Schema, limits::Limits, nodes::Int, named::Int)
    builderdepth() > 0 && return s
    return withconstruction(limits) do budget
        walkstate = vectorbytes(NodeMeta, 16) + frozendictshell() + 64
        reserve!(budget, walkstate)                    # the walk's memo storage and table shells (§4.4)
        walkmetas = Vector{NodeMeta}(undef, 16)
        resize!(walkmetas, 0)
        walk = MetaWalk(walkmetas, 16)
        namedtypes = FrozenDict{String,Schema}()
        allocated!(budget, walkstate)
        collectmetas!(s, walk, namedtypes, limits, budget, 1)
        metas = walk.metas
        length(namedtypes) <= limits.max_named_types ||
            throw(LimitError(:max_named_types, length(namedtypes), limits.max_named_types, :max_named_types, :decode))
        info = GraphInfo(limits, false, false, length(metas), length(namedtypes))
        for (i, m) in enumerate(metas)
            isfilled(m.id) && continue
            fillonce!(m.id, Int32(i - 1))
            fillonce!(m.graph, info)
        end
        checkpublicprint!(s, limits, budget)
        computehashes!(s, metas)
        release!(budget, vectorbytes(NodeMeta, walk.cap) + frozendictbytes(namedtypes) + 64)
        return s
    end
end

"Charge and bound the exact schema text retained properties and defaults will produce."
function checkpublicprint!(s::Schema, limits::Limits, budget::Budget)
    writer = BoundedWriter(budget, limits.max_schema_bytes)
    seen = schemaseen(s, budget)
    printschema(writer, s, "", seen, false, 0)
    releaseseen!(budget, seen)
    return nothing
end

function checkgraphnamebytes(name::AbstractString, limits::Limits)
    sizeof(name) <= limits.max_name_bytes ||
        throw(LimitError(:max_name_bytes, sizeof(name), limits.max_name_bytes, :max_name_bytes, :decode))
    return nothing
end

function checkgraphnames(s::NamedSchema, limits::Limits)
    checkgraphnamebytes(fullname(s), limits)
    for a in s.aliases
        checkgraphnamebytes(a, limits)
    end
    return nothing
end

"The finalisation walk's growable node memo (§4.4 exact replacement across recursive frames)."
mutable struct MetaWalk
    metas::Vector{NodeMeta}
    cap::Int
end

function collectmetas!(s::Schema, walk::MetaWalk, namedtypes::FrozenDict{String,Schema},
                       limits::Limits, budget::Budget, depth::Int)
    depth <= limits.max_schema_depth ||
        throw(LimitError(:max_schema_depth, depth, limits.max_schema_depth, :max_schema_depth, :decode))
    countvalues!(budget)
    any(m -> m === s.meta, walk.metas) && return walk
    length(walk.metas) < limits.max_schema_nodes ||
        throw(LimitError(:max_schema_nodes, length(walk.metas) + 1, limits.max_schema_nodes, :max_schema_nodes, :decode))
    retain!(budget, 160)                              # the caller-built node this graph keeps (parser parity)
    if length(walk.metas) == walk.cap
        newcap = checked_mul(2, walk.cap)
        reserve!(budget, vectorbytes(NodeMeta, newcap))
        replacement = Vector{NodeMeta}(undef, newcap)
        allocated!(budget, vectorbytes(NodeMeta, newcap))
        resize!(replacement, length(walk.metas))
        copyto!(replacement, 1, walk.metas, 1, length(walk.metas))
        oldbytes = vectorbytes(NodeMeta, walk.cap)
        walk.metas = replacement                       # the old memo is unreachable only after the rebind
        walk.cap = newcap
        release!(budget, oldbytes)
    end
    push!(walk.metas, s.meta)
    if s isa NamedSchema
        checkgraphnames(s, limits)
        full = fullname(s)
        if haskey(namedtypes, full)
            namedtypes[full] === s || throw(ArgumentError("named schema \"$full\" is defined more than once"))
        else
            budgetedinsert!(namedtypes, full, s, budget)
        end
    end
    if s isa ArraySchema
        collectmetas!(s.items, walk, namedtypes, limits, budget, depth + 1)
    elseif s isa MapSchema
        collectmetas!(s.values, walk, namedtypes, limits, budget, depth + 1)
    elseif s isa UnionSchema
        length(s.branches) <= limits.max_union_branches ||
            throw(LimitError(:max_union_branches, length(s.branches), limits.max_union_branches, :max_union_branches, :decode))
        foreach(b -> collectmetas!(b, walk, namedtypes, limits, budget, depth + 1), s.branches)
    elseif s isa RecordSchema
        length(s.fields) <= limits.max_fields ||
            throw(LimitError(:max_fields, length(s.fields), limits.max_fields, :max_fields, :decode))
        for f in s.fields
            checkgraphnamebytes(f.name, limits)
            for a in f.aliases
                checkgraphnamebytes(a, limits)
            end
            collectmetas!(f.schema, walk, namedtypes, limits, budget, depth + 1)
        end
    elseif s isa EnumSchema
        length(s.symbols) <= limits.max_enum_symbols ||
            throw(LimitError(:max_enum_symbols, length(s.symbols), limits.max_enum_symbols, :max_enum_symbols, :decode))
        for sym in s.symbols
            checkgraphnamebytes(sym, limits)
        end
    end
    return walk
end

"""
    Avro.juliatype(schema) -> Type

The generic Julia representation of values of `schema` (plan §4.6).
"""
function juliatype end

# ---- complex public constructors (plan §5.1) --------------------------------------------------------

function checkpublicname(name::AbstractString, what::String)
    isvalidname(name) || throw(ArgumentError("invalid $what \"$name\" (must match [A-Za-z_][A-Za-z0-9_]*)"))
    return String(name)
end

function publicnamed(name::AbstractString, namespace::AbstractString, aliases, structural, propsin, logical)
    n, ns = splitfullname(name)
    isempty(ns) || (namespace = ns)
    checkpublicname(n, "name")
    isvalidnamespace(namespace) || throw(ArgumentError("invalid namespace \"$namespace\""))
    full = FullName(n, String(namespace))
    isreservedfullname(full) && throw(ArgumentError("\"$n\" is a primitive type name and cannot be redefined in the null namespace"))
    b = constructionbudget()
    retain!(b, stringbytes(sizeof(n)) + stringbytes(sizeof(namespace)))   # the name copies the node keeps
    raw = BuildBuf{String}(b, 2)
    norm = BuildBuf{String}(b, 2)
    for a in aliases
        a isa Union{AbstractString,Symbol} || throw(ArgumentError("aliases must be strings"))
        abytes = 2 * stringbytes(sizeof(a))
        reserve!(b, abytes)                            # the raw and normalised copies, made next
        sa = String(a)
        na = normalizealias(sa, full.namespace)
        allocated!(b, abytes)
        push!(raw, b, sa)
        found = na == fullname(full)
        if !found
            for j in 1:norm.len
                norm.data[j] == na && (found = true; break)
            end
        end
        found || push!(norm, b, na)
    end
    wrappers = 2 * frozenvectorshell()
    reserve!(b, wrappers)                              # the frozen wrappers, built next
    out = (full, freeze!(FrozenVector{String}(finishbuild!(norm, b), false)),
           freeze!(FrozenVector{String}(finishbuild!(raw, b), false)), makeprops(propsin, structural, logical))
    allocated!(b, wrappers)
    return out
end

"""
    Avro.FixedSchema(name, size; namespace="", logical=nothing, aliases=String[], props=(;), limits=Limits())
"""
function FixedSchema(name::AbstractString, size::Integer; namespace::AbstractString="", logical=nothing, aliases=String[], props=(;), limits::Limits=Limits())
    return withconstruction(limits) do _
        size >= 0 || throw(ArgumentError("fixed size must be ≥ 0"))
        full, norm, raw, p = publicnamed(name, namespace, aliases, SCHEMA_GRAMMAR[:fixed], props, logical)
        s = FixedSchema(full, norm, raw, Int(size), evaluatelogical(:fixed, Int(size), p), p, NodeMeta())
        return finalizepublic!(s, limits, 1, 1)
    end
end

"""
    Avro.EnumSchema(name, symbols; namespace="", default=Avro.nodefault, aliases=String[], doc=nothing, props=(;), limits=Limits())
"""
function EnumSchema(name::AbstractString, symbols; namespace::AbstractString="", default=nodefault, aliases=String[], doc=nothing, props=(;), limits::Limits=Limits())
    return withconstruction(limits) do _
        full, norm, raw, p = publicnamed(name, namespace, aliases, SCHEMA_GRAMMAR[:enum], props, nothing)
        b = constructionbudget()
        shellbytes0 = frozenvectorshell() + frozendictshell()
        reserve!(b, shellbytes0)                       # the symbol vector and index shells (§4.4)
        symsbuf = BuildBuf{String}(b, 4)
        allocated!(b, shellbytes0)
        for x in symbols
            x isa Union{AbstractString,Symbol} || throw(ArgumentError("enum symbols must be strings"))
            symsbuf.len < limits.max_enum_symbols || throw(LimitError(:max_enum_symbols, symsbuf.len + 1, limits.max_enum_symbols, :max_enum_symbols, :encode))
            xbytes = stringbytes(sizeof(x))
            reserve!(b, xbytes)                        # the retained copy, made next
            sym = String(x)
            allocated!(b, xbytes)
            push!(symsbuf, b, sym)
        end
        syms = finishbuild!(symsbuf, b)
        idxbytes = vectorbytes(String, length(syms)) + vectorbytes(Int, length(syms))
        reserve!(b, idxbytes)                          # the exact index capacity (§4.4)
        index = emptywithcapacity(FrozenDict{String,Int}, length(syms))
        allocated!(b, idxbytes)
        for (i, sym) in enumerate(syms)
            checkpublicname(sym, "enum symbol")
            haskey(index, sym) && throw(ArgumentError("duplicate enum symbol \"$sym\""))
            index[sym] = i
        end
        d = nodefault
        if !(default isa NoDefault)
            default isa AbstractString && haskey(index, String(default)) || throw(ArgumentError("enum default must be one of the symbols"))
            dbytes = stringbytes(sizeof(default))
            reserve!(b, dbytes)                        # the retained default copy, made next
            ds = String(default)
            allocated!(b, dbytes)
            jw = BoundedWriter(b, limits.max_schema_bytes)
            escapejson(jw, ds)
            d = DefaultValue(ds, 0, boundedtake!(jw), index[ds], true)
        end
        docbytes = doc === nothing ? 0 : stringbytes(sizeof(doc))
        reserve!(b, docbytes + 24)                     # the doc copy and the symbol wrapper, made next
        s = EnumSchema(full, norm, raw, doc === nothing ? nothing : String(doc), freeze!(FrozenVector{String}(syms, false)), d, freeze!(index), p, NodeMeta())
        allocated!(b, docbytes + 24)
        return finalizepublic!(s, limits, 1, 1)
    end
end

"""
    Avro.ArraySchema(items; props=(;), limits=Limits()) / Avro.MapSchema(values; props=(;), limits=Limits())
"""
function ArraySchema(items::Schema; props=(;), limits::Limits=Limits())
    return withconstruction(limits) do _
        s = withbuilder(() -> ArraySchema(importchild(items), makeprops(props, SCHEMA_GRAMMAR[:array], nothing), NodeMeta()))
        return finalizepublic!(s, limits, 0, 0)
    end
end

function MapSchema(values::Schema; props=(;), limits::Limits=Limits())
    return withconstruction(limits) do _
        s = withbuilder(() -> MapSchema(importchild(values), makeprops(props, SCHEMA_GRAMMAR[:map], nothing), NodeMeta()))
        return finalizepublic!(s, limits, 0, 0)
    end
end

"""
    Avro.UnionSchema(branches; limits=Limits())
"""
function UnionSchema(branches; limits::Limits=Limits())
    return withconstruction(limits) do _
        b = constructionbudget()
        bs = BuildBuf{Schema}(b, 0)
        withbuilder() do
            for branch in branches
                branch isa Schema || throw(ArgumentError("union branches must be schemas"))
                branch isa UnionSchema && throw(ArgumentError("unions may not immediately contain other unions"))
                ident = branchidentity(branch)
                any(i -> branchidentity(bs.data[i]) == ident, 1:bs.len) &&
                    throw(ArgumentError("duplicate union branch $(ident[6:end])"))
                push!(bs, b, importchild(branch))
            end
        end
        bs.len <= limits.max_union_branches || throw(LimitError(:max_union_branches, bs.len, limits.max_union_branches, :max_union_branches, :encode))
        data = finishbuild!(bs, b)
        reserve!(b, 24)
        frozen = freeze!(FrozenVector{Schema}(data, false))
        allocated!(b, 24)
        return finalizepublic!(UnionSchema(frozen, NodeMeta()), limits, 0, 0)
    end
end

"""
    Avro.Field(name, schema; default=Avro.nodefault, order=:ascending, aliases=String[], doc=nothing, props=(;), limits=Limits())

A record field; `default` is a Julia value converted to JSON (`nothing`/`missing` → `null`) and
validated against `schema` with the recursive default rule.
"""
function Field(name::AbstractString, schema::Schema; default=nodefault, order::Symbol=:ascending, aliases=String[], doc=nothing, props=(;), limits::Limits=Limits())
    return withconstruction(limits) do _
        b = constructionbudget()
        checkpublicname(name, "field name")
        order in (:ascending, :descending, :ignore) || throw(ArgumentError("order must be :ascending, :descending or :ignore"))
        als = BuildBuf{String}(b, 2)
        for a in aliases
            a isa Union{AbstractString,Symbol} || throw(ArgumentError("aliases must be strings"))
            abytes = stringbytes(sizeof(a))
            reserve!(b, abytes)                        # the retained copy, made next
            sa = String(a)
            allocated!(b, abytes)
            skip = sa == name
            if !skip
                for j in 1:als.len
                    als.data[j] == sa && (skip = true; break)
                end
            end
            skip || push!(als, b, sa)
        end
        d = nodefault
        if !(default isa NoDefault)
            j = tojsonvalue(default === missing ? nothing : default, b)
            ok, branch = validatedefault(schema, j, limits.max_depth)
            ok || throw(ArgumentError("default value for field \"$name\" does not match its schema"))
            jw = BoundedWriter(b, limits.max_schema_bytes)
            printjson(jw, j, false, 0)
            d = DefaultValue(j, branch, boundedtake!(jw), 0, true)
        end
        fbytes = stringbytes(sizeof(name)) + (doc === nothing ? 0 : stringbytes(sizeof(doc))) + frozenvectorshell() + 128
        reserve!(b, fbytes)                            # name and doc copies, the frozen wrapper and the Field shell, made next
        f = Field(String(name), schema, doc === nothing ? nothing : String(doc), d, order, freeze!(FrozenVector{String}(finishbuild!(als, b), false)), makeprops(props, FIELD_GRAMMAR, nothing))
        allocated!(b, fbytes)
        return f
    end
end

"""
    Avro.RecordSchema(name; namespace="", fields=Avro.Field[], aliases=String[], doc=nothing, iserror=false, props=(;), limits=Limits())
    Avro.RecordSchema(f, name; kw...)

A record; the second form builds a recursive record: `f(ref)` runs with the record registered but
unfilled and returns its fields (an inner `RecordSchema(g, …)` may use outer refs); if `f` throws, the
partial graph is discarded.
"""
function RecordSchema(name::AbstractString; namespace::AbstractString="", fields=Field[], aliases=String[], doc=nothing, iserror::Bool=false, props=(;), limits::Limits=Limits())
    return withconstruction(limits) do _
        return RecordSchema(_ -> fields, name; namespace=namespace, aliases=aliases, doc=doc, iserror=iserror, props=props, limits=limits)
    end
end

function RecordSchema(f, name::AbstractString; namespace::AbstractString="", aliases=String[], doc=nothing, iserror::Bool=false, props=(;), limits::Limits=Limits())
    return withconstruction(limits) do _
        recordschema_(f, name; namespace=namespace, aliases=aliases, doc=doc, iserror=iserror, props=props, limits=limits)
    end
end

function recordschema_(f, name::AbstractString; namespace::AbstractString="", aliases=String[], doc=nothing, iserror::Bool=false, props=(;), limits::Limits=Limits())
    full, norm, raw, p = publicnamed(name, namespace, aliases, SCHEMA_GRAMMAR[:record], props, nothing)
    b = constructionbudget()
    recbytes = frozenvectorshell() + frozendictshell() + 128 + (doc === nothing ? 0 : stringbytes(sizeof(doc)))
    reserve!(b, recbytes)                              # field vector, index and record shells and the doc copy, made next
    fields = FrozenVector{Field}()
    index = FrozenDict{String,Int}()
    rec = RecordSchema(full, norm, raw, doc === nothing ? nothing : String(doc), iserror, p, fields, index, NodeMeta())
    allocated!(b, recbytes)
    withbuilder() do
        fs = f(rec)
        length(fs) <= limits.max_fields || throw(LimitError(:max_fields, length(fs), limits.max_fields, :max_fields, :encode))
        for (i, fld) in enumerate(fs)
            fld isa Field || throw(ArgumentError("fields must be Avro.Field values"))
            haskey(index, fld.name) && throw(ArgumentError("duplicate field name \"$(fld.name)\""))
            reserve!(b, 128)                                              # the rebuilt Field shell, made next
            budgetedpush!(fields, Field(fld.name, importchild(fld.schema), fld.doc, fld.default, fld.order, fld.aliases, fld.props), b)
            allocated!(b, 128)
            budgetedinsert!(index, fld.name, i, b)
        end
    end
    for (i, fa) in enumerate(fields), a in fa.aliases
        haskey(index, a) && throw(ArgumentError("field alias \"$a\" collides with a field name"))
    end
    freeze!(fields)
    freeze!(index)
    return finalizepublic!(rec, limits, 0, 1)
end

"""
    importchild(schema) -> Schema

A child schema passed to a public constructor is used as is when it is not yet finalised (built in the
same constructor call) and deep-copied into the new graph otherwise, so every graph owns its node ids.
"""
function importchild(s::Schema)
    isfilled(s.meta.id) || return s
    memo = importmemo()
    memo !== nothing && return deepcopyschema(s, memo)
    b = constructionbudget()
    reserve!(b, frozendictshell())
    localmemo = FrozenDict{String,Tuple{Schema,Schema}}()
    allocated!(b, frozendictshell())
    try
        return deepcopyschema(s, localmemo)
    finally
        release!(b, frozendictbytes(localmemo))
    end
end

function memocopy(s::Schema, memo::FrozenDict{String,Schema})
    return memo[fullname(s)]
end

function memocopy(s::Schema, memo::FrozenDict{String,Tuple{Schema,Schema}})
    source, copy = memo[fullname(s)]
    source === s || throw(ArgumentError("named schema \"$(fullname(s))\" is defined by more than one child schema"))
    return copy
end

function remembercopy!(memo::FrozenDict{String,Schema}, s::Schema, copy::Schema)
    budgetedinsert!(memo, fullname(s), copy, constructionbudget())
    return copy
end

function remembercopy!(memo::FrozenDict{String,Tuple{Schema,Schema}}, s::Schema, copy::Schema)
    budgetedinsert!(memo, fullname(s), (s, copy), constructionbudget())
    return copy
end

function deepcopyschema(s::Schema, memo::Union{FrozenDict{String,Schema},FrozenDict{String,Tuple{Schema,Schema}}})
    if s isa NamedSchema
        full = fullname(s)
        haskey(memo, full) && return memocopy(s, memo)
    end
    if s isa NullSchema
        return NullSchema(s.props, NodeMeta())
    elseif s isa BooleanSchema
        return BooleanSchema(s.props, NodeMeta())
    elseif s isa IntSchema
        return IntSchema(s.logical, s.props, NodeMeta())
    elseif s isa LongSchema
        return LongSchema(s.logical, s.props, NodeMeta())
    elseif s isa FloatSchema
        return FloatSchema(s.props, NodeMeta())
    elseif s isa DoubleSchema
        return DoubleSchema(s.props, NodeMeta())
    elseif s isa BytesSchema
        return BytesSchema(s.logical, s.props, NodeMeta())
    elseif s isa StringSchema
        return StringSchema(s.logical, s.props, NodeMeta())
    elseif s isa ArraySchema
        return ArraySchema(deepcopyschema(s.items, memo), s.props, NodeMeta())
    elseif s isa MapSchema
        return MapSchema(deepcopyschema(s.values, memo), s.props, NodeMeta())
    elseif s isa UnionSchema
        n = length(s.branches)
        charge = vectorbytes(Schema, n) + 24
        reserve!(constructionbudget(), charge)
        bs = emptywithcapacity(FrozenVector{Schema}, n)
        allocated!(constructionbudget(), charge)
        for b in s.branches
            budgetedpush!(bs, deepcopyschema(b, memo), constructionbudget())
        end
        return UnionSchema(freeze!(bs), NodeMeta())
    elseif s isa FixedSchema
        c = FixedSchema(s.name, s.aliases, s.rawaliases, s.size, s.logical, s.props, NodeMeta())
        remembercopy!(memo, s, c)
        return c
    elseif s isa EnumSchema
        c = EnumSchema(s.name, s.aliases, s.rawaliases, s.doc, s.symbols, s.default, s.symbolindex, s.props, NodeMeta())
        remembercopy!(memo, s, c)
        return c
    else
        n = length(s.fields)
        charge = vectorbytes(Field, n) + 24
        reserve!(constructionbudget(), charge)
        fields = emptywithcapacity(FrozenVector{Field}, n)
        allocated!(constructionbudget(), charge)
        c = RecordSchema(s.name, s.aliases, s.rawaliases, s.doc, s.iserror, s.props, fields, s.fieldindex, NodeMeta())
        remembercopy!(memo, s, c)
        for f in s.fields
            budgetedpush!(fields, Field(f.name, deepcopyschema(f.schema, memo), f.doc, f.default, f.order, f.aliases, f.props), constructionbudget())
        end
        freeze!(fields)
        return c
    end
end

# ---- minsize -------------------------------------------------------------------------------------------

"""
    minsize(schema) -> Int

The minimal encoded size in bytes of a datum of `schema` (`typemax(Int)` when no finite datum exists,
e.g. an empty union, an empty enum, or a required recursive cycle without a nullable/array/map escape);
a memoised, cycle-safe fixed point.
"""
function minsize(s::Schema)
    memo = Vector{Int}(undef, graphinfo(s).nodes)
    fill!(memo, -1)
    return minsize(s, memo, falses(length(memo)))
end

"The budgeted form plan construction uses: the transient memo and active set are charged and released."
function minsize(s::Schema, budget::Budget)
    n = graphinfo(s).nodes
    scratch = vectorbytes(Int, n) + vectorbytes(UInt64, cld(n, 64)) + 32   # the memo, BitVector chunks and shell
    reserve!(budget, scratch)
    memo = Vector{Int}(undef, n)
    active = falses(n)
    allocated!(budget, scratch)
    fill!(memo, -1)
    r = minsize(s, memo, active)
    release!(budget, scratch)                          # construction-only scratch dies here (round-3 item 3)
    return r
end

const INFINITE = typemax(Int)

function satadd(a::Int, b::Int)
    (a == INFINITE || b == INFINITE || a > INFINITE - b) && return INFINITE
    return a + b
end

function minsize(s::Schema, memo::Vector{Int}, active::BitVector)
    id = Int(nodeid(s)) + 1
    memo[id] >= 0 && return memo[id]
    if active[id]
        return INFINITE   # an active recursion edge: this path has no finite datum
    end
    active[id] = true
    r = minsizeof(s, memo, active)
    active[id] = false
    memo[id] = r
    return r
end

function minsizeof(::NullSchema, memo, active)
    return 0
end

function minsizeof(::BooleanSchema, memo, active)
    return 1
end

function minsizeof(::Union{IntSchema,LongSchema}, memo, active)
    return 1
end

function minsizeof(::FloatSchema, memo, active)
    return 4
end

function minsizeof(::DoubleSchema, memo, active)
    return 8
end

function minsizeof(::Union{BytesSchema,StringSchema}, memo, active)
    return 1
end

function minsizeof(::Union{ArraySchema,MapSchema}, memo, active)
    return 1
end

function minsizeof(s::FixedSchema, memo, active)
    return s.size
end

function minsizeof(s::EnumSchema, memo, active)
    return isempty(s.symbols) ? INFINITE : 1
end

function minsizeof(s::UnionSchema, memo, active)
    best = INFINITE
    for (i, b) in enumerate(s.branches)
        m = minsize(b, memo, active)
        m == INFINITE && continue
        best = min(best, satadd(m, varintlength(i - 1)))
    end
    return best
end
function minsizeof(s::RecordSchema, memo, active)
    total = 0
    for f in s.fields
        total = satadd(total, minsize(f.schema, memo, active))
        total == INFINITE && return INFINITE
    end
    return total
end

"""
    varintlength(n) -> Int

The number of bytes of the zig-zag varint encoding of the non-negative integer `n`.
"""
function varintlength(n::Integer)
    v = UInt64(n) << 1
    len = 1
    while v >= 0x80
        v >>= 7
        len += 1
    end
    return len
end
