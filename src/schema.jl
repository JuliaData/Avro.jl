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

NodeMeta() = NodeMeta(FrozenRef{Int32}(), FrozenRef{GraphInfo}(), FrozenRef{UInt64}())

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

kind(::NullSchema) = :null
kind(::BooleanSchema) = :boolean
kind(::IntSchema) = :int
kind(::LongSchema) = :long
kind(::FloatSchema) = :float
kind(::DoubleSchema) = :double
kind(::BytesSchema) = :bytes
kind(::StringSchema) = :string
kind(::ArraySchema) = :array
kind(::MapSchema) = :map
kind(::UnionSchema) = :union
kind(::FixedSchema) = :fixed
kind(::EnumSchema) = :enum
kind(s::RecordSchema) = s.iserror ? :error : :record

logical(s::Union{IntSchema,LongSchema,BytesSchema,StringSchema,FixedSchema}) = s.logical
logical(::Schema) = nothing
props(s::UnionSchema) = Props()
props(s::Schema) = s.props

"""
    Avro.fullname(schema::NamedSchema) -> String
"""
fullname(s::NamedSchema) = fullname(s.name)

nodeid(s::Schema) = s.meta.id[]
graphinfo(s::Schema) = s.meta.graph[]
Base.hash(s::Schema, h::UInt) = hash(s.meta.hash[], h)

"""
    Avro.Schema constructors: Avro.NullSchema(; props), …, Avro.RecordSchema(name; …), Avro.Field(name, schema; …)

Public constructors validate every §4.2 rule, reject `props` keys that collide with the structural keys
the same constructor emits, deep-copy already-frozen children into the new graph, and freeze.
"""
NullSchema(; props=(;)) = build(NullSchema, props)
BooleanSchema(; props=(;)) = build(BooleanSchema, props)
FloatSchema(; props=(;)) = build(FloatSchema, props)
DoubleSchema(; props=(;)) = build(DoubleSchema, props)
IntSchema(; logical=nothing, props=(;)) = build(IntSchema, props; logical=logical)
LongSchema(; logical=nothing, props=(;)) = build(LongSchema, props; logical=logical)
BytesSchema(; logical=nothing, props=(;)) = build(BytesSchema, props; logical=logical)
StringSchema(; logical=nothing, props=(;)) = build(StringSchema, props; logical=logical)

# ---- parse context -----------------------------------------------------------------------------

mutable struct ParseContext
    const limits::Limits
    const budget::Budget
    const allow_invalid_names::Bool
    const allow_invalid_defaults::Bool
    const named::FrozenDict{String,Schema}    # fullname → schema (sorted vector; no hashing)
    const pending::Vector{RecordSchema}       # records registered but not yet filled
    const metas::Vector{NodeMeta}             # in creation order → dense ids
    const namedcount::Base.RefValue{Int}
    const legacyfixednames::Bool              # legacy=:avrojl1: Avro.jl ≤ 1.1.2 wrote fixed schemas without names
    repaired_names::Bool
    repaired_defaults::Bool
    depth::Int
end

function ParseContext(limits::Limits, budget::Budget, allow_invalid_names::Bool, allow_invalid_defaults::Bool, legacyfixednames::Bool=false)
    return ParseContext(limits, budget, allow_invalid_names, allow_invalid_defaults, FrozenDict{String,Schema}(),
        RecordSchema[], NodeMeta[], Ref(0), legacyfixednames, false, false, 0)
end

schemaerror(msg::AbstractString, path::AbstractString) = throw(SchemaError(String(msg), String(path)))

function newmeta!(ctx::ParseContext, path::AbstractString)
    length(ctx.metas) < ctx.limits.max_schema_nodes ||
        throw(LimitError(:max_schema_nodes, length(ctx.metas) + 1, ctx.limits.max_schema_nodes, :max_schema_nodes, :decode))
    reserve!(ctx.budget, 160)
    m = NodeMeta()
    push!(ctx.metas, m)
    return m
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
    p = Props()
    for (i, k) in enumerate(obj.order)
        k in grammar && continue
        reserve!(ctx.budget, sizeof(k) + 32)
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

function stringarrayattr(obj::JSONObject, key::String, path::AbstractString; required::Bool=false)
    haskey(obj, key) || (required ? schemaerror("missing required attribute \"$key\"", path) : return nothing)
    v = obj[key]
    v isa JSONArray || schemaerror("attribute \"$key\" must be a JSON array of strings", string(path, ".", key))
    out = String[]
    for (i, x) in enumerate(v)
        x isa String || schemaerror("attribute \"$key\" must be a JSON array of strings", string(path, ".", key, "[", i - 1, "]"))
        push!(out, x)
    end
    return out
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

escapename(s::AbstractString) = String(chop(sprint(escapejson, s); head=1, tail=1))   # character-wise: the quoted text may end in a multi-byte character

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
                     legacy_fixed_names::Bool=false, budget::Union{Nothing,Budget}=nothing)
    budget === nothing &&
        return withbudget(b -> parseschema(src; allow_invalid_names=allow_invalid_names, allow_invalid_defaults=allow_invalid_defaults,
                                           limits=limits, legacy_fixed_names=legacy_fixed_names, budget=b), limits)
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
sourcebytes(src::Vector{UInt8}, maxbytes::Int, budget::Budget, ::Type{E}) where {E} = src
function sourcebytes(src::AbstractString, maxbytes::Int, budget::Budget, ::Type{E}) where {E}
    reserve!(budget, sizeof(src) + 40)
    return Vector{UInt8}(codeunits(String(src)))
end
function sourcebytes(src::AbstractVector{UInt8}, maxbytes::Int, budget::Budget, ::Type{E}) where {E}
    if src isa SubArray && parent(src) isa Vector{UInt8} && Base.iscontiguous(src)
        return src
    end
    reserve!(budget, length(src) + 40)
    return Vector{UInt8}(src)
end
function sourcebytes(io::IO, maxbytes::Int, budget::Budget, ::Type{E}) where {E}
    readlimit = maxbytes == typemax(Int) ? typemax(Int) : maxbytes + 1
    cap = min(64 * KiB, readlimit)
    reserve!(budget, bytesbytes(cap))
    out = Vector{UInt8}(undef, cap)
    chunkcap = min(64 * KiB, readlimit)
    reserve!(budget, bytesbytes(chunkcap))
    chunk = Vector{UInt8}(undef, chunkcap)
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
            copyto!(replacement, 1, out, 1, len)
            release!(budget, bytesbytes(cap))
            out = replacement
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
        copyto!(exact, 1, out, 1, len)
        release!(budget, bytesbytes(cap))
        out = exact
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
        return primitive(ctx, name, Props(), path)
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
    name == "null" && return NullSchema(p, meta)
    name == "boolean" && return BooleanSchema(p, meta)
    name == "int" && return IntSchema(evaluatelogical(:int, 0, p), p, meta)
    name == "long" && return LongSchema(evaluatelogical(:long, 0, p), p, meta)
    name == "float" && return FloatSchema(p, meta)
    name == "double" && return DoubleSchema(p, meta)
    name == "bytes" && return BytesSchema(evaluatelogical(:bytes, 0, p), p, meta)
    return StringSchema(evaluatelogical(:string, 0, p), p, meta)
end

function parseunion(ctx::ParseContext, arr::JSONArray, enclosing::String, path::String, buf)
    length(arr) <= ctx.limits.max_union_branches ||
        throw(LimitError(:max_union_branches, length(arr), ctx.limits.max_union_branches, :max_union_branches, :decode))
    branches = FrozenVector{Schema}()
    reserve!(ctx.budget, 8 * length(arr) + 40)
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
    return UnionSchema(freeze!(branches), newmeta!(ctx, path))
end

branchidentity(s::NamedSchema) = string("name:", fullname(s))
branchidentity(s::Schema) = string("kind:", kind(s))

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
        return ArraySchema(items, p, newmeta!(ctx, path))
    elseif t == "map"
        haskey(obj, "values") || schemaerror("map schema without \"values\"", path)
        values = parsenode(ctx, obj["values"], enclosing, string(path, ".values"), buf)
        p = collectprops(ctx, obj, SCHEMA_GRAMMAR[:map], path)
        return MapSchema(values, p, newmeta!(ctx, path))
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
    raw = something(stringarrayattr(obj, "aliases", path), String[])
    aliases = String[]
    for (i, a) in enumerate(raw)
        checknamebytes(ctx, a, "alias", string(path, ".aliases[", i - 1, "]"))
        na = normalizealias(a, full.namespace)
        na == fullname(full) && continue                       # self-alias: idempotent, ignored
        na in aliases && continue
        push!(aliases, na)
    end
    return full, freeze!(FrozenVector{String}(aliases, false)), freeze!(FrozenVector{String}(raw, false))
end

function register!(ctx::ParseContext, s::NamedSchema, path::String)
    full = fullname(s)
    for (existing, other) in ctx.named
        other isa NamedSchema || continue
        if full in other.aliases || any(a -> a == existing || a in other.aliases, s.aliases)
            schemaerror("alias collision between \"$full\" and \"$existing\"", path)
        end
    end
    ctx.named[full] = s
    for a in s.aliases
        reserve!(ctx.budget, sizeof(a) + 24)
    end
    return s
end

function parsefixed(ctx::ParseContext, obj::JSONObject, enclosing::String, path::String)
    full, aliases, raw = if ctx.legacyfixednames && !haskey(obj, "name")
        # Avro.jl ≤ 1.1.2 wrote nameless fixed schemas (they carry no references, so a synthetic name is unambiguous)
        (FullName(string("_avrojl1_fixed_", length(ctx.metas) + 1), ""), freeze!(FrozenVector{String}()), freeze!(FrozenVector{String}()))
    else
        parsenamed(ctx, obj, enclosing, path)
    end
    haskey(obj, "size") || schemaerror("fixed schema without \"size\"", path)
    sz = obj["size"]
    sz isa Int64 && sz >= 0 || schemaerror("fixed \"size\" must be a non-negative JSON integer", string(path, ".size"))
    p = collectprops(ctx, obj, SCHEMA_GRAMMAR[:fixed], path)
    s = FixedSchema(full, aliases, raw, Int(sz), evaluatelogical(:fixed, Int(sz), p), p, newmeta!(ctx, path))
    return register!(ctx, s, path)
end

function parseenum(ctx::ParseContext, obj::JSONObject, enclosing::String, path::String, buf)
    full, aliases, raw = parsenamed(ctx, obj, enclosing, path)
    syms = stringarrayattr(obj, "symbols", path; required=true)
    length(syms) <= ctx.limits.max_enum_symbols ||
        throw(LimitError(:max_enum_symbols, length(syms), ctx.limits.max_enum_symbols, :max_enum_symbols, :decode))
    index = FrozenDict{String,Int}()
    for (i, sym) in enumerate(syms)
        spath = string(path, ".symbols[", i - 1, "]")
        checkname(ctx, sym, "enum symbol", spath)
        haskey(index, sym) && schemaerror("duplicate enum symbol \"$(escapename(sym))\"", spath)
        reserve!(ctx.budget, sizeof(sym) + 32)
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
    s = EnumSchema(full, aliases, raw, doc, freeze!(FrozenVector{String}(syms, false)), default, freeze!(index), p, newmeta!(ctx, path))
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
    fields = FrozenVector{Field}()
    fieldindex = FrozenDict{String,Int}()
    rec = RecordSchema(full, aliases, raw, doc, iserror, p, fields, fieldindex, newmeta!(ctx, path))
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
        reserve!(ctx.budget, sizeof(field.name) + 96)
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
    rawaliases = something(stringarrayattr(obj, "aliases", path), String[])
    aliases = String[]
    for (i, a) in enumerate(rawaliases)
        checknamebytes(ctx, a, "field alias", string(path, ".aliases[", i - 1, "]"))
        a == name && continue
        a in aliases || push!(aliases, a)
    end
    default = nodefault
    if haskey(obj, "default")
        default = makedefault(ctx, schema, obj["default"], spanof(obj, "default", buf), string(path, ".default"))
    end
    p = collectprops(ctx, obj, FIELD_GRAMMAR, path)
    return Field(name, schema, doc, default, order, freeze!(FrozenVector{String}(aliases, false)), p)
end

function makedefault(ctx::ParseContext, schema::Schema, json, span::String, path::String)
    reserve!(ctx.budget, sizeof(span) + 64)
    ok, branch = validatedefault(schema, json, ctx.limits.max_depth)
    if ok
        return DefaultValue(json, branch, span, 0, true)
    end
    ctx.allow_invalid_defaults || schemaerror("default value does not match the field's schema", path)
    ctx.repaired_defaults = true
    return DefaultValue(json, 0, span, 0, false)
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

defaultmatches(::NullSchema, json, maxdepth, depth) = json === nothing
defaultmatches(::BooleanSchema, json, maxdepth, depth) = json isa Bool
defaultmatches(::IntSchema, json, maxdepth, depth) = json isa Int64 && typemin(Int32) <= json <= typemax(Int32)
defaultmatches(::LongSchema, json, maxdepth, depth) = json isa Int64
defaultmatches(::Union{FloatSchema,DoubleSchema}, json, maxdepth, depth) =
    json isa Int64 || json isa Float64 || json isa JSONNumber || (json isa String && json in ("NaN", "Infinity", "-Infinity"))
defaultmatches(::BytesSchema, json, maxdepth, depth) = json isa String && isbytestring(json)
defaultmatches(s::FixedSchema, json, maxdepth, depth) = json isa String && isbytestring(json) && bytestringlength(json) == s.size
defaultmatches(::StringSchema, json, maxdepth, depth) = json isa String && isstrictutf8(json)
defaultmatches(s::EnumSchema, json, maxdepth, depth) = json isa String && haskey(s.symbolindex, json)
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

bytestringlength(s::AbstractString) = length(s)

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

propshash(p::Props, h::UInt) = hash(p.vals, hash(p.keys, h))
logicalhash(::Nothing, h::UInt) = h
logicalhash(l::DecimalLogical, h::UInt) = hash(l.scale, hash(l.precision, hash(:decimal, h)))
logicalhash(l::LogicalType, h::UInt) = hash(logicalname(l), h)

function structuralhash(s::PrimitiveSchema, inprogress)
    return propshash(s.props, logicalhash(logical(s), hash(kind(s), UInt(0xa7))))
end
structuralhash(s::ArraySchema, inprogress) = propshash(s.props, hash(schemahash(s.items, inprogress), hash(:array, UInt(0xa7))))
structuralhash(s::MapSchema, inprogress) = propshash(s.props, hash(schemahash(s.values, inprogress), hash(:map, UInt(0xa7))))
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
defaulthash(::NoDefault, h::UInt) = hash(:nodefault, h)
defaulthash(d::DefaultValue, h::UInt) = d.valid ? hash(d.branch, hash(d.json, h)) : hash(d.span, hash(:invalid, h))
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
    visited = Vector{Vector{Int32}}()   # per a-node sorted partner ids
    return schemaequal(a, b, visited, budget)
end

larger(a::Limits, b::Limits) = a.max_resolution_work >= b.max_resolution_work ? a : b

function visitpair!(visited::Vector{Vector{Int32}}, a::Schema, b::Schema, budget::Budget)
    ia = Int(nodeid(a)) + 1
    while length(visited) < ia
        push!(visited, Int32[])
    end
    partners = visited[ia]
    ib = nodeid(b)
    i = searchsortedfirst(partners, ib)
    addresolution!(budget, 1 + (i <= length(partners) ? 1 : 0))
    i <= length(partners) && partners[i] == ib && return true
    insert!(partners, i, ib)
    addresolution!(budget, length(partners) - i + 1)
    return false
end

function schemaequal(a::Schema, b::Schema, visited, budget)
    a === b && return true
    typeof(a) === typeof(b) || return false
    visitpair!(visited, a, b, budget) && return true
    return structuralequal(a, b, visited, budget)
end

propsequal(a::Props, b::Props) = a.keys == b.keys && a.vals == b.vals
structuralequal(a::PrimitiveSchema, b::PrimitiveSchema, visited, budget) = logical(a) == logical(b) && propsequal(a.props, b.props)
structuralequal(a::ArraySchema, b::ArraySchema, visited, budget) = propsequal(a.props, b.props) && schemaequal(a.items, b.items, visited, budget)
structuralequal(a::MapSchema, b::MapSchema, visited, budget) = propsequal(a.props, b.props) && schemaequal(a.values, b.values, visited, budget)
function structuralequal(a::UnionSchema, b::UnionSchema, visited, budget)
    length(a.branches) == length(b.branches) || return false
    for (x, y) in zip(a.branches, b.branches)
        schemaequal(x, y, visited, budget) || return false
    end
    return true
end
structuralequal(a::FixedSchema, b::FixedSchema, visited, budget) =
    a.name == b.name && a.size == b.size && a.aliases.data == b.aliases.data && a.logical == b.logical && propsequal(a.props, b.props)
defaultequal(a::NoDefault, b::NoDefault) = true
defaultequal(a::DefaultValue, b::DefaultValue) = a.valid == b.valid && (a.valid ? (a.branch == b.branch && a.json == b.json) : a.span == b.span)
defaultequal(a::Default, b::Default) = false
structuralequal(a::EnumSchema, b::EnumSchema, visited, budget) =
    a.name == b.name && a.symbols.data == b.symbols.data && a.aliases.data == b.aliases.data && a.doc == b.doc &&
    defaultequal(a.default, b.default) && propsequal(a.props, b.props)
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

Base.:(==)(a::DecimalLogical, b::DecimalLogical) = a.precision == b.precision && a.scale == b.scale
Base.:(==)(a::UnknownLogical, b::UnknownLogical) = a.name == b.name

# ---- printing ---------------------------------------------------------------------------------------

"""
    Avro.json(schema; pretty=false) -> String

The schema as spec JSON: the first occurrence of a named type in full, later references by fullname,
the namespace attribute only when it differs from the enclosing one, custom `props` re-emitted (numbers
verbatim, strings re-escaped), and defaults by their exact source text.
"""
function json(s::Schema; pretty::Bool=false)
    io = IOBuffer()
    printschema(io, s, "", FrozenDict{String,Bool}(), pretty, 0)
    return String(take!(io))
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

function printschema(io::IO, s::Schema, enclosing::String, seen::FrozenDict{String,Bool}, pretty::Bool, level::Int)
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
        full = fullname(s)
        if haskey(seen, full)
            escapejson(io, s.name.namespace == enclosing ? s.name.name : full)
            return nothing
        end
        seen[full] = true
        printnamed(io, s, enclosing, seen, pretty, level)
    end
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

printdefault(io::IO, d::DefaultValue) = isempty(d.span) ? printjson(io, d.json, false, 0) : print(io, d.span)

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

Base.show(io::IO, s::Schema) = print(io, "Avro.Schema(", json(s), ")")

function Base.show(io::IO, ::MIME"text/plain", s::Schema)
    print(io, "Avro.Schema ", json(s; pretty=true))
    return nothing
end

# ---- public constructors ---------------------------------------------------------------------------

function build(::Type{T}, propsin; logical=nothing, limits::Limits=Limits()) where {T<:PrimitiveSchema}
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
    p = Props()
    kvs = propsin isa NamedTuple ? (String(k) => v for (k, v) in pairs(propsin)) : propsin
    for (k, v) in kvs
        ks = String(k)
        ks in structural && throw(ArgumentError("`props` key \"$ks\" collides with a structural attribute emitted by this constructor"))
        logical !== nothing && ks in ("logicalType", "precision", "scale") &&
            throw(ArgumentError("`props` key \"$ks\" is synthesised by `logical=`"))
        haskey(p, ks) && throw(ArgumentError("duplicate `props` key \"$ks\""))
        p[ks] = tojsonvalue(v)
    end
    if logical !== nothing
        p["logicalType"] = logicalname(logical)
        if logical isa DecimalLogical
            p["precision"] = Int64(logical.precision)
            p["scale"] = Int64(logical.scale)
        end
    end
    return freeze!(p)
end

tojsonvalue(x::Union{Nothing,Bool,Int64,String,JSONNumber,JSONArray,JSONObject}) = x
tojsonvalue(x::Integer) = Int64(x)
tojsonvalue(x::AbstractFloat) = isfinite(x) ? JSONNumber(repr(Float64(x))) : (isnan(x) ? "NaN" : (x > 0 ? "Infinity" : "-Infinity"))
tojsonvalue(x::AbstractString) = String(x)
tojsonvalue(x::Symbol) = String(x)
function tojsonvalue(x::AbstractVector)
    v = FrozenVector{Any}()
    for e in x
        push!(v, tojsonvalue(e))
    end
    return JSONArray(freeze!(v))
end
function tojsonvalue(x::Union{AbstractDict,NamedTuple})
    m = FrozenDict{String,Any}()
    order = FrozenVector{String}()
    for (k, v) in (x isa NamedTuple ? pairs(x) : x)
        ks = String(k)
        haskey(m, ks) && throw(ArgumentError("duplicate key \"$ks\""))
        m[ks] = tojsonvalue(v)
        push!(order, ks)
    end
    return JSONObject(freeze!(m), freeze!(order))
end

# Nested public constructors inside a recursive builder must not finalise the graph early (the record
# under construction is still unfilled); the outermost builder finalises everything. Every outermost
# constructor call owns one import memo, so the same finalised child passed twice is one definition
# plus references.
builderdepth() = get(task_local_storage(), :avro_builder_depth, 0)::Int
importmemo() = get(task_local_storage(), :avro_import_memo, nothing)

function withbuilder(f)
    outer = builderdepth() == 0
    task_local_storage(:avro_builder_depth, builderdepth() + 1)
    outer && task_local_storage(:avro_import_memo, FrozenDict{String,Schema}())
    try
        return f()
    finally
        task_local_storage(:avro_builder_depth, builderdepth() - 1)
        outer && task_local_storage(:avro_import_memo, nothing)
    end
end

function finalizepublic!(s::Schema, limits::Limits, nodes::Int, named::Int)
    builderdepth() > 0 && return s
    metas = NodeMeta[]
    namedcount = Ref(0)
    collectmetas!(s, metas, namedcount)
    info = GraphInfo(limits, false, false, length(metas), namedcount[])
    for (i, m) in enumerate(metas)
        isfilled(m.id) && continue
        fillonce!(m.id, Int32(i - 1))
        fillonce!(m.graph, info)
    end
    computehashes!(s, metas)
    return s
end

function collectmetas!(s::Schema, metas::Vector{NodeMeta}, namedcount::Base.RefValue{Int})
    any(m -> m === s.meta, metas) && return metas
    push!(metas, s.meta)
    s isa NamedSchema && (namedcount[] += 1)
    if s isa ArraySchema
        collectmetas!(s.items, metas, namedcount)
    elseif s isa MapSchema
        collectmetas!(s.values, metas, namedcount)
    elseif s isa UnionSchema
        foreach(b -> collectmetas!(b, metas, namedcount), s.branches)
    elseif s isa RecordSchema
        foreach(f -> collectmetas!(f.schema, metas, namedcount), s.fields)
    end
    return metas
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
    raw = String[String(a) for a in aliases]
    norm = String[]
    for a in raw
        na = normalizealias(a, full.namespace)
        (na == fullname(full) || na in norm) && continue
        push!(norm, na)
    end
    return full, freeze!(FrozenVector{String}(norm, false)), freeze!(FrozenVector{String}(raw, false)), makeprops(propsin, structural, logical)
end

"""
    Avro.FixedSchema(name, size; namespace="", logical=nothing, aliases=String[], props=(;), limits=Limits())
"""
function FixedSchema(name::AbstractString, size::Integer; namespace::AbstractString="", logical=nothing, aliases=String[], props=(;), limits::Limits=Limits())
    size >= 0 || throw(ArgumentError("fixed size must be ≥ 0"))
    full, norm, raw, p = publicnamed(name, namespace, aliases, SCHEMA_GRAMMAR[:fixed], props, logical)
    s = FixedSchema(full, norm, raw, Int(size), evaluatelogical(:fixed, Int(size), p), p, NodeMeta())
    return finalizepublic!(s, limits, 1, 1)
end

"""
    Avro.EnumSchema(name, symbols; namespace="", default=Avro.nodefault, aliases=String[], doc=nothing, props=(;), limits=Limits())
"""
function EnumSchema(name::AbstractString, symbols; namespace::AbstractString="", default=nodefault, aliases=String[], doc=nothing, props=(;), limits::Limits=Limits())
    full, norm, raw, p = publicnamed(name, namespace, aliases, SCHEMA_GRAMMAR[:enum], props, nothing)
    syms = String[String(x) for x in symbols]
    length(syms) <= limits.max_enum_symbols || throw(LimitError(:max_enum_symbols, length(syms), limits.max_enum_symbols, :max_enum_symbols, :encode))
    index = FrozenDict{String,Int}()
    for (i, sym) in enumerate(syms)
        checkpublicname(sym, "enum symbol")
        haskey(index, sym) && throw(ArgumentError("duplicate enum symbol \"$sym\""))
        index[sym] = i
    end
    d = nodefault
    if !(default isa NoDefault)
        default isa AbstractString && haskey(index, String(default)) || throw(ArgumentError("enum default must be one of the symbols"))
        d = DefaultValue(String(default), 0, sprint(escapejson, String(default)), index[String(default)], true)
    end
    s = EnumSchema(full, norm, raw, doc === nothing ? nothing : String(doc), freeze!(FrozenVector{String}(syms, false)), d, freeze!(index), p, NodeMeta())
    return finalizepublic!(s, limits, 1, 1)
end

"""
    Avro.ArraySchema(items; props=(;), limits=Limits()) / Avro.MapSchema(values; props=(;), limits=Limits())
"""
function ArraySchema(items::Schema; props=(;), limits::Limits=Limits())
    s = withbuilder(() -> ArraySchema(importchild(items), makeprops(props, SCHEMA_GRAMMAR[:array], nothing), NodeMeta()))
    return finalizepublic!(s, limits, 0, 0)
end

function MapSchema(values::Schema; props=(;), limits::Limits=Limits())
    s = withbuilder(() -> MapSchema(importchild(values), makeprops(props, SCHEMA_GRAMMAR[:map], nothing), NodeMeta()))
    return finalizepublic!(s, limits, 0, 0)
end

"""
    Avro.UnionSchema(branches; limits=Limits())
"""
function UnionSchema(branches; limits::Limits=Limits())
    bs = FrozenVector{Schema}()
    withbuilder() do
        for b in branches
            b isa Schema || throw(ArgumentError("union branches must be schemas"))
            b isa UnionSchema && throw(ArgumentError("unions may not immediately contain other unions"))
            ident = branchidentity(b)
            any(x -> branchidentity(x) == ident, bs) && throw(ArgumentError("duplicate union branch $(ident[6:end])"))
            push!(bs, importchild(b))
        end
    end
    length(bs) <= limits.max_union_branches || throw(LimitError(:max_union_branches, length(bs), limits.max_union_branches, :max_union_branches, :encode))
    return finalizepublic!(UnionSchema(freeze!(bs), NodeMeta()), limits, 0, 0)
end

"""
    Avro.Field(name, schema; default=Avro.nodefault, order=:ascending, aliases=String[], doc=nothing, props=(;), limits=Limits())

A record field; `default` is a Julia value converted to JSON (`nothing`/`missing` → `null`) and
validated against `schema` with the recursive default rule.
"""
function Field(name::AbstractString, schema::Schema; default=nodefault, order::Symbol=:ascending, aliases=String[], doc=nothing, props=(;), limits::Limits=Limits())
    checkpublicname(name, "field name")
    order in (:ascending, :descending, :ignore) || throw(ArgumentError("order must be :ascending, :descending or :ignore"))
    als = String[]
    for a in aliases
        (String(a) == name || String(a) in als) && continue
        push!(als, String(a))
    end
    d = nodefault
    if !(default isa NoDefault)
        j = tojsonvalue(default === missing ? nothing : default)
        ok, branch = validatedefault(schema, j, limits.max_depth)
        ok || throw(ArgumentError("default value for field \"$name\" does not match its schema"))
        d = DefaultValue(j, branch, sprint(printjson, j, false, 0), 0, true)
    end
    return Field(String(name), schema, doc === nothing ? nothing : String(doc), d, order, freeze!(FrozenVector{String}(als, false)), makeprops(props, FIELD_GRAMMAR, nothing))
end

"""
    Avro.RecordSchema(name; namespace="", fields=Avro.Field[], aliases=String[], doc=nothing, iserror=false, props=(;), limits=Limits())
    Avro.RecordSchema(f, name; kw...)

A record; the second form builds a recursive record: `f(ref)` runs with the record registered but
unfilled and returns its fields (an inner `RecordSchema(g, …)` may use outer refs); if `f` throws, the
partial graph is discarded.
"""
function RecordSchema(name::AbstractString; namespace::AbstractString="", fields=Field[], aliases=String[], doc=nothing, iserror::Bool=false, props=(;), limits::Limits=Limits())
    return RecordSchema(_ -> fields, name; namespace=namespace, aliases=aliases, doc=doc, iserror=iserror, props=props, limits=limits)
end

function RecordSchema(f, name::AbstractString; namespace::AbstractString="", aliases=String[], doc=nothing, iserror::Bool=false, props=(;), limits::Limits=Limits())
    full, norm, raw, p = publicnamed(name, namespace, aliases, SCHEMA_GRAMMAR[:record], props, nothing)
    fields = FrozenVector{Field}()
    index = FrozenDict{String,Int}()
    rec = RecordSchema(full, norm, raw, doc === nothing ? nothing : String(doc), iserror, p, fields, index, NodeMeta())
    withbuilder() do
        fs = f(rec)
        length(fs) <= limits.max_fields || throw(LimitError(:max_fields, length(fs), limits.max_fields, :max_fields, :encode))
        for (i, fld) in enumerate(fs)
            fld isa Field || throw(ArgumentError("fields must be Avro.Field values"))
            haskey(index, fld.name) && throw(ArgumentError("duplicate field name \"$(fld.name)\""))
            push!(fields, Field(fld.name, importchild(fld.schema), fld.doc, fld.default, fld.order, fld.aliases, fld.props))
            index[fld.name] = i
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
    return deepcopyschema(s, memo === nothing ? FrozenDict{String,Schema}() : memo)
end

function deepcopyschema(s::Schema, memo::FrozenDict{String,Schema})
    if s isa NamedSchema
        full = fullname(s)
        haskey(memo, full) && return memo[full]
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
        bs = FrozenVector{Schema}()
        for b in s.branches
            push!(bs, deepcopyschema(b, memo))
        end
        return UnionSchema(freeze!(bs), NodeMeta())
    elseif s isa FixedSchema
        c = FixedSchema(s.name, s.aliases, s.rawaliases, s.size, s.logical, s.props, NodeMeta())
        memo[fullname(s)] = c
        return c
    elseif s isa EnumSchema
        c = EnumSchema(s.name, s.aliases, s.rawaliases, s.doc, s.symbols, s.default, s.symbolindex, s.props, NodeMeta())
        memo[fullname(s)] = c
        return c
    else
        fields = FrozenVector{Field}()
        c = RecordSchema(s.name, s.aliases, s.rawaliases, s.doc, s.iserror, s.props, fields, s.fieldindex, NodeMeta())
        memo[fullname(s)] = c
        for f in s.fields
            push!(fields, Field(f.name, deepcopyschema(f.schema, memo), f.doc, f.default, f.order, f.aliases, f.props))
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

minsizeof(::NullSchema, memo, active) = 0
minsizeof(::BooleanSchema, memo, active) = 1
minsizeof(::Union{IntSchema,LongSchema}, memo, active) = 1
minsizeof(::FloatSchema, memo, active) = 4
minsizeof(::DoubleSchema, memo, active) = 8
minsizeof(::Union{BytesSchema,StringSchema}, memo, active) = 1
minsizeof(::Union{ArraySchema,MapSchema}, memo, active) = 1
minsizeof(s::FixedSchema, memo, active) = s.size
minsizeof(s::EnumSchema, memo, active) = isempty(s.symbols) ? INFINITE : 1
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
