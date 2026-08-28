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
mutable struct GraphInfo
    const limits::Limits
    const repaired_names::Bool
    const repaired_defaults::Bool
    const nodes::Int
    const namedtypes::Int
end

struct RawMember
    key::String
    value::String
end

struct SchemaLexemes
    members::FrozenDict{String,RawMember}
    fields::FrozenVector{FrozenDict{String,RawMember}}
end

const EMPTY_RAW_MEMBERS = freeze!(FrozenDict{String,RawMember}())
const EMPTY_FIELD_LEXEMES = freeze!(FrozenVector{FrozenDict{String,RawMember}}())
const EMPTY_SCHEMA_LEXEMES = SchemaLexemes(EMPTY_RAW_MEMBERS, EMPTY_FIELD_LEXEMES)

"""
    NodeMeta

Write-once node identity: a dense graph-local `id`, the shared `GraphInfo`, and the structural `hash`.
"""
struct NodeMeta
    id::FrozenRef{Int32}
    graph::FrozenRef{GraphInfo}
    hash::FrozenRef{UInt64}
    lexemes::FrozenRef{SchemaLexemes}
end

function NodeMeta()
    return NodeMeta(FrozenRef{Int32}(), FrozenRef{GraphInfo}(), FrozenRef{UInt64}(),
                    FrozenRef{SchemaLexemes}())
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
    return EMPTY_PROPS
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

function schemalexemes(s::Schema)
    return isfilled(s.meta.lexemes) ? s.meta.lexemes[] : EMPTY_SCHEMA_LEXEMES
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

const PARSE_PATH_KEY = UInt8(1)
const PARSE_PATH_INDEX = UInt8(2)

"One allocation-free component in the current schema JSON path."
struct ParsePathSegment
    kind::UInt8
    key::Union{Nothing,String}
    index::Int
end

const ParsePath = BuildBuf{ParsePathSegment}

mutable struct ParseContext
    const limits::Limits
    const budget::Budget
    const allow_invalid_names::Bool
    const allow_invalid_defaults::Bool
    const named::FrozenDict{String,Schema}    # fullname → schema (sorted vector; §4.4 replacement growth)
    const identities::FrozenDict{String,Schema} # every fullname and alias, for global uniqueness
    const pending::Vector{RecordSchema}       # records registered but not yet filled (≤ max_depth, prebuilt)
    metas::Vector{NodeMeta}                   # in creation order → dense ids (§4.4 replacement growth)
    metascap::Int
    const namedcount::Base.RefValue{Int}
    const legacyfixednames::Bool              # legacy=:avrojl1: Avro.jl ≤ 1.1.2 wrote fixed schemas without names
    legacyfixedcount::Int                     # nameless-fixed ordinal within this document
    repaired_names::Bool
    repaired_defaults::Bool
    depth::Int
    const path::ParsePath
end

function ParseContext(limits::Limits, budget::Budget, allow_invalid_names::Bool, allow_invalid_defaults::Bool, legacyfixednames::Bool=false)
    metascap = 16
    state = vectorbytes(RecordSchema, limits.max_schema_depth) + vectorbytes(NodeMeta, metascap) +
            frozendictshell() + 128   # pending, metas, context and both table shells
    reserve!(budget, state)
    pending = Vector{RecordSchema}(undef, limits.max_schema_depth)   # the fill stack never exceeds max_schema_depth (§4.4)
    resize!(pending, 0)
    metas = Vector{NodeMeta}(undef, metascap)
    resize!(metas, 0)
    path = ParsePath(budget, min(limits.max_schema_depth, 16))
    ctx = ParseContext(limits, budget, allow_invalid_names, allow_invalid_defaults,
        FrozenDict{String,Schema}(), FrozenDict{String,Schema}(), pending, metas, metascap,
        Ref(0), legacyfixednames, 0, false, false, 0, path)
    allocated!(budget, state)
    return ctx
end

function pushparsekey!(ctx::ParseContext, key::String)
    push!(ctx.path, ctx.budget, ParsePathSegment(PARSE_PATH_KEY, key, 0))
    return nothing
end

function pushparseindex!(ctx::ParseContext, index::Int)
    push!(ctx.path, ctx.budget, ParsePathSegment(PARSE_PATH_INDEX, nothing, index))
    return nothing
end

function popparsepath!(ctx::ParseContext)
    ctx.path.len > 0 || throw(ArgumentError("cannot pop an empty schema path"))
    ctx.path.len -= 1
    return nothing
end

function parsepathbytes(path::ParsePath)
    n = 1
    for i in 1:path.len
        segment = path.data[i]
        n = segment.kind == PARSE_PATH_KEY ?
            checked_add(n, checked_add(1, sizeof(segment.key::String))) :
            checked_add(n, checked_add(2, integerdigits(segment.index)))
    end
    return n
end

"Materialise an exact schema JSON path only when an error escapes."
function formatparsepath(ctx::ParseContext)
    n = parsepathbytes(ctx.path)
    buf = diagnosticbuffer(n, ctx.budget)
    pos = 1
    buf[pos] = UInt8('$')
    pos += 1
    for i in 1:ctx.path.len
        segment = ctx.path.data[i]
        if segment.kind == PARSE_PATH_KEY
            buf[pos] = UInt8('.')
            pos = writediagnosticpart!(buf, pos + 1, segment.key::String)
        else
            buf[pos] = UInt8('[')
            pos = writediagnosticpart!(buf, pos + 1, segment.index)
            buf[pos] = UInt8(']')
            pos += 1
        end
    end
    pos == n + 1 || throw(ArgumentError("schema path size mismatch"))
    return finishdiagnostic!(buf, ctx.budget)
end

function schemaerror(ctx::ParseContext, msg::AbstractString)
    throw(SchemaError(String(msg), formatparsepath(ctx)))
end

function schemaerror(ctx::ParseContext, msg::AbstractString, key::String)
    pushparsekey!(ctx, key)
    try
        schemaerror(ctx, msg)
    finally
        popparsepath!(ctx)
    end
end

function schemaerror(ctx::ParseContext, msg::AbstractString, key::String, index::Int)
    pushparsekey!(ctx, key)
    pushparseindex!(ctx, index)
    try
        schemaerror(ctx, msg)
    finally
        popparsepath!(ctx)
        popparsepath!(ctx)
    end
end

# Shared frozen empties: frozen containers are immutable by contract, so every node may reference the
# same empty instance instead of allocating one (uncharged: process-global constants, plan §4.6).
const EMPTY_PROPS = freeze!(Props())
const EMPTY_STRING_LIST = freeze!(FrozenVector{String}())

const NODE_META_BYTES = 128   # the NodeMeta object and its four write-once refs
const NODE_SHELL_BYTES = 64   # the schema node object, settled by `settlednode` after construction
const GRAPH_INFO_BYTES = 240  # one shared heap object: Limits plus flags and graph counts

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

function newmeta!(ctx::ParseContext)
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

function collectprops(ctx::ParseContext, obj::JSONObject, grammar)
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
        budgetedinsert!(p, k, obj[k], ctx.budget)
    end
    return freeze!(p)
end

function stringattr(ctx::ParseContext, obj::JSONObject, key::String; required::Bool=false)
    haskey(obj, key) || (required ? schemaerror(ctx, "missing required attribute \"$key\"") : return nothing)
    v = obj[key]
    v isa String || schemaerror(ctx, "attribute \"$key\" must be a JSON string", key)
    return v
end

function stringarrayattr(ctx::ParseContext, obj::JSONObject, key::String; required::Bool=false)
    haskey(obj, key) || (required ? schemaerror(ctx, "missing required attribute \"$key\"") : return nothing)
    v = obj[key]
    v isa JSONArray || schemaerror(ctx, "attribute \"$key\" must be a JSON array of strings", key)
    out = BuildBuf{String}(ctx.budget, length(v))
    for (i, x) in enumerate(v)
        x isa String || schemaerror(ctx, "attribute \"$key\" must be a JSON array of strings", key, i - 1)
        push!(out, ctx.budget, x)
    end
    return finishbuild!(out, ctx.budget)
end

function checknamebytes(ctx::ParseContext, s::AbstractString)
    sizeof(s) <= ctx.limits.max_name_bytes ||
        throw(LimitError(:max_name_bytes, sizeof(s), ctx.limits.max_name_bytes, :max_name_bytes, :decode))
    return nothing
end

function checkname(ctx::ParseContext, s::AbstractString, what::AbstractString,
                   key::Union{Nothing,String}=nothing, index::Union{Nothing,Int}=nothing)
    checknamebytes(ctx, s)
    isvalidname(s) && return nothing
    if !ctx.allow_invalid_names
        msg = "invalid $what \"$(escapename(s))\" (must match [A-Za-z_][A-Za-z0-9_]*)"
        key === nothing ? schemaerror(ctx, msg) :
            index === nothing ? schemaerror(ctx, msg, key) : schemaerror(ctx, msg, key, index)
    end
    ctx.repaired_names = true
    return nothing
end

function checknamespace(ctx::ParseContext, s::AbstractString, key::Union{Nothing,String}=nothing)
    checknamebytes(ctx, s)
    isvalidnamespace(s) && return nothing
    ctx.allow_invalid_names || (key === nothing ?
        schemaerror(ctx, "invalid namespace \"$(escapename(s))\"") :
        schemaerror(ctx, "invalid namespace \"$(escapename(s))\"", key))
    ctx.repaired_names = true
    return nothing
end

const DIAGNOSTIC_NAME_PREFIX_BYTES = 96

function escapedprefix(io::IO, s::AbstractString, stop::Int)
    cu = codeunits(s)
    i = 1
    while i <= stop
        b = cu[i]
        if b == UInt8('"')
            print(io, "\\\""); i += 1
        elseif b == UInt8('\\')
            print(io, "\\\\"); i += 1
        elseif b < 0x20
            if b == 0x08; print(io, "\\b") elseif b == 0x0C; print(io, "\\f") elseif b == 0x0A; print(io, "\\n")
            elseif b == 0x0D; print(io, "\\r") elseif b == 0x09; print(io, "\\t")
            else writehexescape(io, b) end
            i += 1
        elseif b == 0xED && i + 2 <= stop && cu[i + 1] >= 0xA0 &&
               (cu[i + 1] & 0xC0) == 0x80 && (cu[i + 2] & 0xC0) == 0x80
            cp = (UInt32(b & 0x0F) << 12) |
                 (UInt32(cu[i + 1] & 0x3F) << 6) | UInt32(cu[i + 2] & 0x3F)
            writehexescape(io, cp)
            i += 3
        else
            Base.write(io, b)
            i += 1
        end
    end
    return nothing
end

"Escape a bounded prefix for diagnostics; never copy an attacker-sized name into an error."
function escapename(s::AbstractString)
    n = sizeof(s)
    stop = min(n, DIAGNOSTIC_NAME_PREFIX_BYTES)
    cu = codeunits(s)
    while stop < n && stop > 0 && (cu[stop + 1] & 0xC0) == 0x80
        stop -= 1
    end
    prefix = sprint(io -> escapedprefix(io, s, stop); sizehint=6 * stop)
    stop == n && return prefix
    return string(prefix, "… (+", n - stop, " bytes)")
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
    addinput!(budget, length(buf))
    errfn = (msg, pos) -> throw(SchemaError(string(msg, " (byte ", pos, ")"), "\$"))
    limitfn = (limit, observed, value) -> throw(LimitError(limit, observed, value, limit, :decode))
    tree = parsejson(buf; maxbytes=limits.max_schema_bytes, maxdepth=limits.max_schema_depth, errfn=errfn, budget=budget,
        limitfn=limitfn, bytelimit=:max_schema_bytes, depthlimit=:max_schema_depth)
    ctx = ParseContext(limits, budget, allow_invalid_names, allow_invalid_defaults, legacy_fixed_names)
    s = parsenode(ctx, tree, "", buf)
    isempty(ctx.pending) || schemaerror(ctx, "internal error: unfilled record")
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
    n = sizeof(src)
    checksourcebytes(E, n, maxbytes)
    charge = bytesbytes(n)
    reserve!(budget, charge)
    out = Vector{UInt8}(undef, n)
    allocated!(budget, charge)
    copyto!(out, 1, codeunits(src), 1, n)
    return out
end

function sourcebytes(src::AbstractVector{UInt8}, maxbytes::Int, budget::Budget, ::Type{E}) where {E}
    if src isa SubArray && parent(src) isa Vector{UInt8} && Base.iscontiguous(src)
        return src
    end
    n = length(src)
    checksourcebytes(E, n, maxbytes)
    charge = bytesbytes(n)
    reserve!(budget, charge)
    out = Vector{UInt8}(undef, n)
    allocated!(budget, charge)
    copyto!(out, 1, src, 1, n)
    return out
end

function checksourcebytes(::Type{E}, observed::Int, maximum::Int) where {E}
    observed <= maximum && return nothing
    limit = E === SchemaError ? :max_schema_bytes : :max_datum_bytes
    throw(LimitError(limit, observed, maximum, limit, :decode))
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

function parsenode(ctx::ParseContext, node, enclosing::String, buf)
    ctx.depth += 1
    ctx.depth <= ctx.limits.max_schema_depth || throw(LimitError(:max_schema_depth, ctx.depth, ctx.limits.max_schema_depth, :max_schema_depth, :decode))
    try
        if node isa String
            return parsereference(ctx, node, enclosing)
        elseif node isa JSONArray
            return parseunion(ctx, node, enclosing, buf)
        elseif node isa JSONObject
            schema = parseobjectschema(ctx, node, enclosing, buf)
            isfilled(schema.meta.lexemes) ||
                fillonce!(schema.meta.lexemes,
                    capturelexemes(ctx, node, buf, schema))
            return schema
        else
            schemaerror(ctx, "a schema must be a JSON string, object or array")
        end
    finally
        ctx.depth -= 1
    end
end

function parsenode(ctx::ParseContext, node, enclosing::String, buf, key::String)
    pushparsekey!(ctx, key)
    try
        return parsenode(ctx, node, enclosing, buf)
    finally
        popparsepath!(ctx)
    end
end

function parsereference(ctx::ParseContext, name::String, enclosing::String)
    if name in PRIMITIVE_NAMES
        return primitive(ctx, name, EMPTY_PROPS)
    end
    checknamebytes(ctx, name)
    dotted = findlast('.', name) !== nothing
    key = dotted || isempty(enclosing) ? name : FullName(name, enclosing)
    s = budgetedget(ctx.named, key, nothing, ctx.budget)
    s === nothing && !isempty(enclosing) &&
        (s = budgetedget(ctx.named, name, nothing, ctx.budget))   # a bare reference may name a null-namespace type
    s === nothing && schemaerror(ctx, "undefined type \"$(escapename(name))\" (types must be defined before use)")
    return s
end

function primitive(ctx::ParseContext, name::String, p::Props)
    meta = newmeta!(ctx)
    name == "null" && return settlednode(ctx, NullSchema(p, meta))
    name == "boolean" && return settlednode(ctx, BooleanSchema(p, meta))
    name == "int" && return settlednode(ctx, IntSchema(evaluatelogical(:int, 0, p), p, meta))
    name == "long" && return settlednode(ctx, LongSchema(evaluatelogical(:long, 0, p), p, meta))
    name == "float" && return settlednode(ctx, FloatSchema(p, meta))
    name == "double" && return settlednode(ctx, DoubleSchema(p, meta))
    name == "bytes" && return settlednode(ctx, BytesSchema(evaluatelogical(:bytes, 0, p), p, meta))
    return settlednode(ctx, StringSchema(evaluatelogical(:string, 0, p), p, meta))
end

"A non-allocating union identity: named branches use their fullname; all other branches use their kind."
struct BranchIdentity
    tag::UInt8
    name::String
    namespace::String
end

function Base.:(==)(a::BranchIdentity, b::BranchIdentity)
    a.tag == b.tag || return false
    a.tag == 0 || return true
    return fullnameequal(FullName(a.name, a.namespace), FullName(b.name, b.namespace))
end

function Base.isless(a::BranchIdentity, b::BranchIdentity)
    a.tag == b.tag || return a.tag < b.tag
    a.tag == 0 || return false
    return isless(FullName(a.name, a.namespace), FullName(b.name, b.namespace))
end

function keycomparisonwork(a::BranchIdentity, b::BranchIdentity)
    a.tag == b.tag || return 2
    a.tag == 0 || return 2
    return 1 + fullnamecomparisonwork(FullName(a.name, a.namespace),
                                      FullName(b.name, b.namespace))
end

function branchkindtag(s::Schema)
    k = kind(s)
    k === :null && return UInt8(1)
    k === :boolean && return UInt8(2)
    k === :int && return UInt8(3)
    k === :long && return UInt8(4)
    k === :float && return UInt8(5)
    k === :double && return UInt8(6)
    k === :bytes && return UInt8(7)
    k === :string && return UInt8(8)
    k === :array && return UInt8(9)
    k === :map && return UInt8(10)
    throw(ArgumentError("internal error: unsupported union branch kind $k"))
end

function branchidentity(s::NamedSchema)
    return BranchIdentity(0, s.name.name, s.name.namespace)
end

function branchidentity(s::Schema)
    return BranchIdentity(branchkindtag(s), "", "")
end

function branchidentitylabel(s::NamedSchema)
    return fullname(s)
end

function branchidentitylabel(s::Schema)
    return string(kind(s))
end

function parseunion(ctx::ParseContext, arr::JSONArray, enclosing::String, buf)
    length(arr) <= ctx.limits.max_union_branches ||
        throw(LimitError(:max_union_branches, length(arr), ctx.limits.max_union_branches, :max_union_branches, :decode))
    branchbytes = vectorbytes(Schema, length(arr)) + 24
    reserve!(ctx.budget, branchbytes)              # the exact branch capacity and frozen shell (§4.4)
    branches = emptywithcapacity(FrozenVector{Schema}, length(arr))
    allocated!(ctx.budget, branchbytes)
    reserve!(ctx.budget, frozendictshell())
    seen = FrozenDict{BranchIdentity,Int}()
    allocated!(ctx.budget, frozendictshell())
    try
        for (i, b) in enumerate(arr)
            pushparseindex!(ctx, i - 1)
            try
                b isa JSONArray && schemaerror(ctx, "unions may not immediately contain other unions")
                s = parsenode(ctx, b, enclosing, buf)
                s isa UnionSchema && schemaerror(ctx, "unions may not immediately contain other unions")
                ident = branchidentity(s)
                budgetedhaskey(seen, ident, ctx.budget) &&
                    schemaerror(ctx, "duplicate union branch $(branchidentitylabel(s))")
                budgetedinsert!(seen, ident, i, ctx.budget)
                push!(branches, s)
            finally
                popparsepath!(ctx)
            end
        end
    finally
        release!(ctx.budget, frozendictbytes(seen))
    end
    return settlednode(ctx, UnionSchema(freeze!(branches), newmeta!(ctx)))
end

function parseobjectschema(ctx::ParseContext, obj::JSONObject, enclosing::String, buf)
    haskey(obj, "type") || schemaerror(ctx, "schema object without a \"type\" attribute")
    t = obj["type"]
    t isa String || schemaerror(ctx, "a schema object's \"type\" must be a JSON string naming a primitive or one of record, error, enum, array, map, fixed", "type")
    if t in PRIMITIVE_NAMES
        p = collectprops(ctx, obj, ("type",))
        return primitive(ctx, t, p)
    elseif t == "array"
        haskey(obj, "items") || schemaerror(ctx, "array schema without \"items\"")
        items = parsenode(ctx, obj["items"], enclosing, buf, "items")
        p = collectprops(ctx, obj, SCHEMA_GRAMMAR[:array])
        return settlednode(ctx, ArraySchema(items, p, newmeta!(ctx)))
    elseif t == "map"
        haskey(obj, "values") || schemaerror(ctx, "map schema without \"values\"")
        values = parsenode(ctx, obj["values"], enclosing, buf, "values")
        p = collectprops(ctx, obj, SCHEMA_GRAMMAR[:map])
        return settlednode(ctx, MapSchema(values, p, newmeta!(ctx)))
    elseif t == "record" || t == "error"
        return parserecord(ctx, obj, enclosing, buf, t == "error")
    elseif t == "enum"
        return parseenum(ctx, obj, enclosing, buf)
    elseif t == "fixed"
        return parsefixed(ctx, obj, enclosing)
    else
        schemaerror(ctx, "a schema object's \"type\" must name a primitive or one of record, error, enum, array, map, fixed (a named-type reference is a schema string, a union is an array); got \"$(escapename(t))\"", "type")
    end
end

function parsenamed(ctx::ParseContext, obj::JSONObject, enclosing::String)
    name = stringattr(ctx, obj, "name"; required=true)
    namespace = haskey(obj, "namespace") ? obj["namespace"] : nothing
    namespace === nothing || namespace isa String || schemaerror(ctx, "attribute \"namespace\" must be a JSON string", "namespace")
    checknamebytes(ctx, name)
    full = resolveparsedfullname(name, namespace, enclosing, ctx.budget)
    checkname(ctx, full.name, "name", "name")
    checknamespace(ctx, full.namespace, "namespace")
    isreservedfullname(full) && schemaerror(ctx, "\"$(full.name)\" is a primitive type name and cannot be redefined in the null namespace", "name")
    budgetedhaskey(ctx.named, full, ctx.budget) &&
        schemaerror(ctx, "duplicate type name", "name")
    ctx.namedcount[] += 1
    ctx.namedcount[] <= ctx.limits.max_named_types ||
        throw(LimitError(:max_named_types, ctx.namedcount[], ctx.limits.max_named_types, :max_named_types, :decode))
    raw = stringarrayattr(ctx, obj, "aliases")
    if raw === nothing || isempty(raw)
        return full, EMPTY_STRING_LIST, EMPTY_STRING_LIST
    end
    aliases = BuildBuf{String}(ctx.budget, length(raw))
    reserve!(ctx.budget, frozendictshell())
    seen = FrozenDict{String,Bool}()
        allocated!(ctx.budget, frozendictshell())
    try
        for (i, a) in enumerate(raw)
            checknamebytes(ctx, a)
            normalized = findlast('.', a) === nothing && !isempty(full.namespace) ?
                checked_add(checked_add(sizeof(full.namespace), 1), sizeof(a)) : sizeof(a)
            normalized <= ctx.limits.max_name_bytes ||
                throw(LimitError(:max_name_bytes, normalized, ctx.limits.max_name_bytes,
                                 :max_name_bytes, ctx.budget.direction))
            na = ownednormalizedalias(a, full.namespace, ctx.budget)
            if fullnameequal(full, na)
                release!(ctx.budget, stringbytes(sizeof(na)))
                continue
            end
            if budgetedhaskey(seen, na, ctx.budget)
                release!(ctx.budget, stringbytes(sizeof(na)))
                continue
            end
            budgetedinsert!(seen, na, true, ctx.budget)
            push!(aliases, ctx.budget, na)
        end
    finally
        release!(ctx.budget, frozendictbytes(seen))
    end
    reserve!(ctx.budget, 48)                                   # the two frozen wrappers
    wrapped = (full, freeze!(FrozenVector{String}(finishbuild!(aliases, ctx.budget), false)),
               freeze!(FrozenVector{String}(raw, false)))
    allocated!(ctx.budget, 48)
    return wrapped
end

function register!(ctx::ParseContext, s::NamedSchema)
    budgetedhaskey(ctx.identities, s.name, ctx.budget) &&
        schemaerror(ctx, "a name or alias is used by more than one type")
    for alias in s.aliases
        budgetedhaskey(ctx.identities, alias, ctx.budget) &&
            schemaerror(ctx, "name or alias \"$alias\" is used by more than one type")
    end
    full = ownedfullname(s.name, ctx.budget)
    budgetedinsert!(ctx.named, full, s, ctx.budget)
    budgetedinsert!(ctx.identities, full, s, ctx.budget)
    for alias in s.aliases
        budgetedinsert!(ctx.identities, alias, s, ctx.budget)
    end
    return s
end

"Apply the fullname algorithm to parser-owned strings without an uncharged dotted-name split."
function resolveparsedfullname(name::String, namespace::Union{Nothing,String}, enclosing::String,
                               budget::Budget)
    dot = findlast('.', name)
    dot === nothing && return FullName(name, namespace === nothing ? enclosing : namespace)
    first = nextind(name, dot)
    simple = ownedstringcopy(name, first, sizeof(name) - dot, budget)
    qualifier = ownedstringcopy(name, firstindex(name), dot - 1, budget)
    return FullName(simple, qualifier)
end

function parsefixed(ctx::ParseContext, obj::JSONObject, enclosing::String)
    full, aliases, raw = if ctx.legacyfixednames && !haskey(obj, "name")
        # Avro.jl ≤ 1.1.2 wrote nameless fixed schemas (they carry no references, so a synthetic name is unambiguous)
        ctx.legacyfixedcount = checked_add(ctx.legacyfixedcount, 1)
        (FullName(string("_avrojl1_fixed_", ctx.legacyfixedcount), ""), freeze!(FrozenVector{String}()), freeze!(FrozenVector{String}()))
    else
        parsenamed(ctx, obj, enclosing)
    end
    haskey(obj, "size") || schemaerror(ctx, "fixed schema without \"size\"")
    sz = obj["size"]
    sz isa Int64 && sz >= 0 || schemaerror(ctx, "fixed \"size\" must be a non-negative JSON integer", "size")
    p = collectprops(ctx, obj, SCHEMA_GRAMMAR[:fixed])
    s = settlednode(ctx, FixedSchema(full, aliases, raw, Int(sz), evaluatelogical(:fixed, Int(sz), p), p, newmeta!(ctx)))
    return register!(ctx, s)
end

function parseenum(ctx::ParseContext, obj::JSONObject, enclosing::String, buf)
    full, aliases, raw = parsenamed(ctx, obj, enclosing)
    syms = stringarrayattr(ctx, obj, "symbols"; required=true)
    length(syms) <= ctx.limits.max_enum_symbols ||
        throw(LimitError(:max_enum_symbols, length(syms), ctx.limits.max_enum_symbols, :max_enum_symbols, :decode))
    indexbytes = vectorbytes(String, length(syms)) + vectorbytes(Int, length(syms)) + 32
    reserve!(ctx.budget, indexbytes)               # the exact symbol-index capacity and shell (§4.4)
    index = emptywithcapacity(FrozenDict{String,Int}, length(syms))
    allocated!(ctx.budget, indexbytes)
    for (i, sym) in enumerate(syms)
        checkname(ctx, sym, "enum symbol", "symbols", i - 1)
        budgetedhaskey(index, sym, ctx.budget) &&
            schemaerror(ctx, "duplicate enum symbol \"$(escapename(sym))\"", "symbols", i - 1)
        retain!(ctx.budget, sizeof(sym) + 32)      # the symbol string the schema keeps alive
        budgetedinsert!(index, sym, i, ctx.budget)
    end
    doc = stringattr(ctx, obj, "doc")
    default = nodefault
    if haskey(obj, "default")
        d = obj["default"]
        if d isa String && budgetedhaskey(index, d, ctx.budget)
            default = DefaultValue(d, 0, spanof(ctx, obj, "default", buf),
                budgetedgetindex(index, d, ctx.budget), true)
        else
            ctx.allow_invalid_defaults || schemaerror(ctx, "enum default must be one of the symbols", "default")
            ctx.repaired_defaults = true
            default = DefaultValue(d, 0, spanof(ctx, obj, "default", buf), 0, false)
        end
    end
    p = collectprops(ctx, obj, SCHEMA_GRAMMAR[:enum])
    s = settlednode(ctx, EnumSchema(full, aliases, raw, doc, freeze!(FrozenVector{String}(syms, false)), default, freeze!(index), p, newmeta!(ctx)))
    return register!(ctx, s)
end

function spanof(ctx::ParseContext, obj::JSONObject, key::String, buf)
    isempty(obj.spans) && return ""
    for (i, k) in enumerate(obj.order)
        k == key || continue
        r = obj.spans[i].value
        return rawspan(ctx, r, buf)
    end
    return ""
end

function rawspan(ctx::ParseContext, range::UnitRange{Int}, buf)
    n = length(range)
    charge = stringbytes(n)
    reserve!(ctx.budget, charge)
    span = GC.@preserve buf unsafe_string(pointer(buf, first(range)), n)
    allocated!(ctx.budget, charge)
    return span
end

function rawmembers(ctx::ParseContext, obj::JSONObject, buf, skipvalues=())
    n = length(obj.order)
    n == 0 && return EMPTY_RAW_MEMBERS
    charge = 48 + vectorbytes(String, n) + vectorbytes(RawMember, n)
    reserve!(ctx.budget, charge)
    members = emptywithcapacity(FrozenDict{String,RawMember}, n)
    allocated!(ctx.budget, charge)
    for i in 1:n
        decoded = ownedstringcopy(obj.order[i], ctx.budget)
        rawkey = rawspan(ctx, obj.spans[i].key, buf)
        rawvalue = decoded in skipvalues ? "" : rawspan(ctx, obj.spans[i].value, buf)
        budgetedinsert!(members, decoded, RawMember(rawkey, rawvalue), ctx.budget)
    end
    return freeze!(members)
end

function capturelexemes(ctx::ParseContext, obj::JSONObject, buf, schema::Schema)
    skipped = schema isa ArraySchema ? ("items",) : schema isa MapSchema ? ("values",) :
              schema isa RecordSchema ? ("fields",) : ()
    members = rawmembers(ctx, obj, buf, skipped)
    schema isa RecordSchema || return SchemaLexemes(members, EMPTY_FIELD_LEXEMES)
    fieldjson = budgetedget(obj.members, "fields", nothing, ctx.budget)
    fieldjson isa JSONArray || return SchemaLexemes(members, EMPTY_FIELD_LEXEMES)
    n = length(fieldjson)
    n == 0 && return SchemaLexemes(members, EMPTY_FIELD_LEXEMES)
    charge = 24 + vectorbytes(FrozenDict{String,RawMember}, n)
    reserve!(ctx.budget, charge)
    fields = emptywithcapacity(FrozenVector{FrozenDict{String,RawMember}}, n)
    allocated!(ctx.budget, charge)
    for field in fieldjson
        field isa JSONObject || continue
        push!(fields, rawmembers(ctx, field, buf, ("type",)))
    end
    return SchemaLexemes(members, freeze!(fields))
end

function parserecord(ctx::ParseContext, obj::JSONObject, enclosing::String, buf, iserror::Bool)
    full, aliases, raw = parsenamed(ctx, obj, enclosing)
    haskey(obj, "fields") || schemaerror(ctx, "record schema without \"fields\"")
    farr = obj["fields"]
    farr isa JSONArray || schemaerror(ctx, "\"fields\" must be a JSON array", "fields")
    length(farr) <= ctx.limits.max_fields || throw(LimitError(:max_fields, length(farr), ctx.limits.max_fields, :max_fields, :decode))
    doc = stringattr(ctx, obj, "doc")
    p = collectprops(ctx, obj, SCHEMA_GRAMMAR[:record])
    nfields = length(farr)
    fieldstate = vectorbytes(Field, nfields) + vectorbytes(String, nfields) + vectorbytes(Int, nfields) + 56
    reserve!(ctx.budget, fieldstate)               # exact field-vector and index capacity plus shells (§4.4)
    fields = emptywithcapacity(FrozenVector{Field}, nfields)
    fieldindex = emptywithcapacity(FrozenDict{String,Int}, nfields)
    allocated!(ctx.budget, fieldstate)
    reserve!(ctx.budget, frozendictshell())
    aliasowners = FrozenDict{String,Int}()
    allocated!(ctx.budget, frozendictshell())
    rec = settlednode(ctx, RecordSchema(full, aliases, raw, doc, iserror, p, fields, fieldindex, newmeta!(ctx)))
    register!(ctx, rec)                            # register before filling so self-references resolve
    push!(ctx.pending, rec)
    ns = full.namespace
    try
        for (i, f) in enumerate(farr)
            pushparsekey!(ctx, "fields")
            pushparseindex!(ctx, i - 1)
            try
                f isa JSONObject || schemaerror(ctx, "each field must be a JSON object")
                field = parsefield(ctx, f, ns, buf)
                budgetedhaskey(fieldindex, field.name, ctx.budget) &&
                    schemaerror(ctx, "duplicate field name \"$(escapename(field.name))\"", "name")
                budgetedhaskey(aliasowners, field.name, ctx.budget) &&
                    schemaerror(ctx, "field name \"$(escapename(field.name))\" collides with an earlier field alias", "name")
                for alias in field.aliases
                    budgetedhaskey(fieldindex, alias, ctx.budget) &&
                        schemaerror(ctx, "field alias \"$(escapename(alias))\" collides with a field name", "aliases")
                    budgetedhaskey(aliasowners, alias, ctx.budget) &&
                        schemaerror(ctx, "field alias \"$(escapename(alias))\" is declared by two fields", "aliases")
                    budgetedinsert!(aliasowners, alias, i, ctx.budget)
                end
                retain!(ctx.budget, sizeof(field.name) + 96)   # the field name, Field shell and index entry kept alive
                push!(fields, field)
                budgetedinsert!(fieldindex, field.name, i, ctx.budget)
            finally
                popparsepath!(ctx)
                popparsepath!(ctx)
            end
        end
    finally
        release!(ctx.budget, frozendictbytes(aliasowners))
    end
    freeze!(fields)
    freeze!(fieldindex)
    pop!(ctx.pending)
    return rec
end

function parsefield(ctx::ParseContext, obj::JSONObject, ns::String, buf)
    name = stringattr(ctx, obj, "name"; required=true)
    checkname(ctx, name, "field name", "name")
    haskey(obj, "type") || schemaerror(ctx, "field without \"type\"")
    schema = parsenode(ctx, obj["type"], ns, buf, "type")
    doc = stringattr(ctx, obj, "doc")
    order = :ascending
    if haskey(obj, "order")
        o = obj["order"]
        o isa String && o in ("ascending", "descending", "ignore") || schemaerror(ctx, "\"order\" must be \"ascending\", \"descending\" or \"ignore\"", "order")
        order = Symbol(o)
    end
    rawaliases = stringarrayattr(ctx, obj, "aliases")
    if rawaliases === nothing || isempty(rawaliases)
        rawaliases = String[]
    end
    aliases = BuildBuf{String}(ctx.budget, length(rawaliases))
    reserve!(ctx.budget, frozendictshell())
    seen = FrozenDict{String,Bool}()
        allocated!(ctx.budget, frozendictshell())
    try
        for (i, a) in enumerate(rawaliases)
            checknamebytes(ctx, a)
            a == name && continue
            budgetedhaskey(seen, a, ctx.budget) && continue
            budgetedinsert!(seen, a, true, ctx.budget)
            push!(aliases, ctx.budget, a)
        end
    finally
        release!(ctx.budget, frozendictbytes(seen))
    end
    default = nodefault
    if haskey(obj, "default")
        pushparsekey!(ctx, "default")
        try
            default = makedefault(ctx, schema, obj["default"], spanof(ctx, obj, "default", buf))
        finally
            popparsepath!(ctx)
        end
    end
    p = collectprops(ctx, obj, FIELD_GRAMMAR)
    if aliases.len == 0
        return Field(name, schema, doc, default, order, EMPTY_STRING_LIST, p)
    end
    reserve!(ctx.budget, 24)                                   # the frozen wrapper (the Field shell is retained by its record)
    f = Field(name, schema, doc, default, order, freeze!(FrozenVector{String}(finishbuild!(aliases, ctx.budget), false)), p)
    allocated!(ctx.budget, 24)
    return f
end

function makedefault(ctx::ParseContext, schema::Schema, json, span::String)
    ok, branch = validatedefault(schema, json, ctx.limits.max_depth, 1,
                                 ctx.budget)
    reserve!(ctx.budget, 64)
    d = ok ? DefaultValue(json, branch, span, 0, true) : begin
        ctx.allow_invalid_defaults || schemaerror(ctx, "default value does not match the field's schema")
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
function validatedefault(schema::Schema, json, maxdepth::Int, depth::Int=1,
                         budget::Union{Nothing,Budget}=nothing)
    budget === nothing || addresolution!(budget)
    depth <= maxdepth &&
        return (defaultmatches(schema, json, maxdepth, depth, budget), 0)
    return (false, 0)
end

function validatedefault(schema::UnionSchema, json, maxdepth::Int, depth::Int=1,
                         budget::Union{Nothing,Budget}=nothing)
    budget === nothing || addresolution!(budget)
    depth <= maxdepth || return (false, 0)
    for (i, b) in enumerate(schema.branches)
        budget === nothing || addresolution!(budget)
        defaultmatches(b, json, maxdepth, depth, budget) && return (true, i)
    end
    return (false, 0)
end

function defaultmatches(schema::Schema, json, maxdepth, depth)
    return defaultmatches(schema, json, maxdepth, depth, nothing)
end

function defaultmatches(::NullSchema, json, maxdepth, depth, budget)
    return json === nothing
end

function defaultmatches(::BooleanSchema, json, maxdepth, depth, budget)
    return json isa Bool
end

function defaultmatches(s::IntSchema, json, maxdepth, depth, budget)
    json isa Int64 && typemin(Int32) <= json <= typemax(Int32) || return false
    s.logical isa TimeMillis && return 0 <= json < 86_400_000
    return true
end

function defaultmatches(s::LongSchema, json, maxdepth, depth, budget)
    json isa Int64 || return false
    s.logical isa TimeMicros && return 0 <= json < 86_400_000_000
    return true
end

function defaultmatches(::Union{FloatSchema,DoubleSchema}, json, maxdepth, depth,
                        budget)
    return json isa Int64 || json isa Float64 || json isa JSONNumber || (json isa String && json in ("NaN", "Infinity", "-Infinity"))
end

function defaultmatches(s::BytesSchema, json, maxdepth, depth, budget)
    json isa String && isbytestring(json) || return false
    s.logical isa DecimalLogical &&
        return decimaldefaultmatches(json, s.logical, budget)
    return true
end

function defaultmatches(s::FixedSchema, json, maxdepth, depth, budget)
    json isa String && isbytestring(json) && bytestringlength(json) == s.size ||
        return false
    s.logical isa DecimalLogical &&
        return decimaldefaultmatches(json, s.logical, budget)
    return true
end

function defaultmatches(s::StringSchema, json, maxdepth, depth, budget)
    json isa String && isstrictutf8(json) || return false
    s.logical isa UUIDLogical && return tryparseuuid(json) !== nothing
    return true
end

function defaultmatches(s::EnumSchema, json, maxdepth, depth, budget)
    return json isa String && haskey(s.symbolindex, json)
end

function defaultmatches(s::ArraySchema, json, maxdepth, depth, budget)
    json isa JSONArray || return false
    for x in json
        validatedefault(s.items, x, maxdepth, depth + 1, budget)[1] || return false
    end
    return true
end

function defaultmatches(s::MapSchema, json, maxdepth, depth, budget)
    json isa JSONObject || return false
    for (k, v) in json.members
        isstrictutf8(k) || return false
        validatedefault(s.values, v, maxdepth, depth + 1, budget)[1] || return false
    end
    return true
end

function defaultmatches(s::RecordSchema, json, maxdepth, depth, budget)
    json isa JSONObject || return false
    for f in s.fields
        if haskey(json, f.name)
            validatedefault(f.schema, json[f.name], maxdepth, depth + 1,
                            budget)[1] || return false
        else
            f.default isa DefaultValue && f.default.valid || return false
        end
    end
    return true       # unknown members are ignored (the span is retained verbatim)
end

function defaultmatches(s::UnionSchema, json, maxdepth, depth, budget)
    return validatedefault(s, json, maxdepth, depth, budget)[1]
end

"Validate a decimal byte-string default without retaining its temporary magnitude."
function decimaldefaultmatches(json::String, logical::DecimalLogical,
                               budget::Union{Nothing,Budget})
    n = length(json)
    n > 0 || return false
    if n <= 16
        value = Int128(0)
        for char in json
            value = (value << 8) | Int128(UInt8(char))
        end
        shift = 8 * (16 - n)
        value = (value << shift) >> shift
        return ndigits128(value) <= logical.precision
    end

    checkpoint = budget === nothing ? nothing : budgetcheckpoint(budget)
    bytes = nothing
    magnitude = nothing
    bytecharge = bytesbytes(n)
    bigcharge = widedecimalbytes(n) - 16
    try
        budget === nothing || reserve!(budget, bytecharge)
        bytes = Vector{UInt8}(undef, n)
        budget === nothing || allocated!(budget, bytecharge)
        for (index, char) in enumerate(json)
            bytes[index] = UInt8(char)
        end
        if bytes[1] >= 0x80
            for index in eachindex(bytes)
                bytes[index] = ~bytes[index]
            end
            carry = true
            for index in reverse(eachindex(bytes))
                carry || break
                bytes[index], carry = Base.add_with_overflow(bytes[index], UInt8(1))
            end
        end
        budget === nothing || reserve!(budget, bigcharge)
        magnitude = BigInt(; nbits=checked_mul(8, n))
        budget === nothing || allocated!(budget, bigcharge)
        importdecimal!(magnitude, bytes, 1, n, false)
        valid = ndigits(magnitude) <= logical.precision
        magnitude = nothing
        bytes = nothing
        budget === nothing || release!(budget, bytecharge + bigcharge)
        return valid
    catch
        magnitude = nothing
        bytes = nothing
        budget === nothing || rollbackreservations!(budget,
                                                     checkpoint::NTuple{2,Int})
        rethrow()
    end
end

"Active schema/default pairs used to reject recursive defaults with no finite expansion."
mutable struct DefaultValidationState
    const budget::Budget
    const maxdepth::Int
    const schemas::BuildBuf{Schema}
    const json::BuildBuf{Any}
end

function DefaultValidationState(budget::Budget, maxdepth::Int)
    checkpoint = budgetcheckpoint(budget)
    try
        return DefaultValidationState(budget, maxdepth,
                                      BuildBuf{Schema}(budget, 8),
                                      BuildBuf{Any}(budget, 8))
    catch
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

function releasedefaultvalidation!(state::DefaultValidationState)
    schemas = state.schemas
    json = state.json
    release!(state.budget,
             vectorbytes(Schema, length(schemas.data)) +
             vectorbytes(Any, length(json.data)) +
             shellbytes(BuildBuf{Schema}) + shellbytes(BuildBuf{Any}))
    return nothing
end

function activedefault(state::DefaultValidationState, schema::Schema, json)
    for index in 1:state.schemas.len
        addresolution!(state.budget)
        @inbounds state.schemas.data[index] === schema &&
                  state.json.data[index] === json && return true
    end
    return false
end

"Validate against the complete graph and recompute the selected union branch."
function completevalidatedefault(schema::Schema, json,
                                 state::DefaultValidationState,
                                 depth::Int=1)
    addresolution!(state.budget)
    depth <= state.maxdepth || return (false, 0)
    activedefault(state, schema, json) && return (false, 0)
    push!(state.schemas, state.budget, schema)
    try
        push!(state.json, state.budget, json)
        try
            return completevalidatedefaultimpl(schema, json, state, depth)
        finally
            state.json.len -= 1
        end
    finally
        state.schemas.len -= 1
    end
end

function completevalidatedefaultimpl(schema::Schema, json,
                                     state::DefaultValidationState,
                                     depth::Int)
    return (completedefaultmatches(schema, json, state, depth), 0)
end

function completevalidatedefaultimpl(schema::UnionSchema, json,
                                     state::DefaultValidationState,
                                     depth::Int)
    for (index, branch) in enumerate(schema.branches)
        addresolution!(state.budget)
        completevalidatedefault(branch, json, state, depth)[1] &&
            return (true, index)
    end
    return (false, 0)
end

function completedefaultmatches(schema::Schema, json,
                                state::DefaultValidationState, depth::Int)
    return defaultmatches(schema, json, state.maxdepth, depth, state.budget)
end

function completedefaultmatches(schema::ArraySchema, json,
                                state::DefaultValidationState, depth::Int)
    json isa JSONArray || return false
    for item in json
        completevalidatedefault(schema.items, item, state, depth + 1)[1] ||
            return false
    end
    return true
end

function completedefaultmatches(schema::MapSchema, json,
                                state::DefaultValidationState, depth::Int)
    json isa JSONObject || return false
    for key in json.order
        isstrictutf8(key) || return false
        child = budgetedgetindex(json.members, key, state.budget)
        completevalidatedefault(schema.values, child, state, depth + 1)[1] ||
            return false
    end
    return true
end

function completedefaultmatches(schema::RecordSchema, json,
                                state::DefaultValidationState, depth::Int)
    json isa JSONObject || return false
    for field in schema.fields
        child, childschema = if budgetedhaskey(json.members, field.name,
                                              state.budget)
            (budgetedgetindex(json.members, field.name, state.budget),
             field.schema)
        else
            default = field.default
            default isa DefaultValue || return false
            (default.json, field.schema)
        end
        completevalidatedefault(childschema, child, state, depth + 1)[1] ||
            return false
    end
    return true
end

function replacedefault!(record::RecordSchema, index::Int, branch::Int,
                         valid::Bool, budget::Budget)
    field = record.fields[index]
    old = field.default
    old isa DefaultValue || throw(ArgumentError("cannot replace a missing default"))
    old.branch == branch && old.valid == valid && return nothing
    reserve!(budget, 64)
    replacement = DefaultValue(old.json, branch, old.span, old.index, valid)
    allocated!(budget, 64)
    record.fields.data[index] = Field(field.name, field.schema, field.doc,
                                      replacement, field.order, field.aliases,
                                      field.props)
    old = nothing
    release!(budget, 64)
    return nothing
end

function invalidgraphdefault!(ctx::ParseContext, record::RecordSchema,
                              field::Field)
    ctx.allow_invalid_defaults && return false
    schemaerror(ctx, diagnosticstring(
        ctx.budget, "default value for field ", boundedquoted(field.name),
        " of record ", record.name, " does not match its complete schema"))
end

function invalidgraphdefault!(::Nothing, record::RecordSchema, field::Field)
    throw(ArgumentError("default value for field \"$(field.name)\" of record $(record.name) does not match its complete schema"))
end

function validategraphdefaultnode!(schema::Schema, seen::Vector{Bool},
                                   state::DefaultValidationState,
                                   ctx::Union{Nothing,ParseContext})
    index = Int(nodeid(schema)) + 1
    seen[index] && return nothing
    seen[index] = true
    addresolution!(state.budget)
    if schema isa ArraySchema
        validategraphdefaultnode!(schema.items, seen, state, ctx)
    elseif schema isa MapSchema
        validategraphdefaultnode!(schema.values, seen, state, ctx)
    elseif schema isa UnionSchema
        for branch in schema.branches
            validategraphdefaultnode!(branch, seen, state, ctx)
        end
    elseif schema isa RecordSchema
        for (fieldindex, field) in enumerate(schema.fields)
            if field.default isa DefaultValue
                valid, branch = completevalidatedefault(
                    field.schema, field.default.json, state)
                if valid
                    replacedefault!(schema, fieldindex, branch, true,
                                    state.budget)
                else
                    invalidgraphdefault!(ctx, schema, field)
                    ctx === nothing || (ctx.repaired_defaults = true)
                    replacedefault!(schema, fieldindex, 0, false,
                                    state.budget)
                end
            end
            validategraphdefaultnode!(field.schema, seen, state, ctx)
        end
    end
    return nothing
end

function validategraphdefaults!(root::Schema, nodes::Int, limits::Limits,
                                budget::Budget,
                                ctx::Union{Nothing,ParseContext}=nothing)
    checkpoint = budgetcheckpoint(budget)
    seen = nothing
    state = nothing
    try
        charge = vectorbytes(Bool, nodes)
        reserve!(budget, charge)
        seen = Vector{Bool}(undef, nodes)
        allocated!(budget, charge)
        fill!(seen, false)
        state = DefaultValidationState(budget, limits.max_depth)
        validategraphdefaultnode!(root, seen, state, ctx)
        releasedefaultvalidation!(state)
        state = nothing
        seen = nothing
        release!(budget, charge)
        return root
    catch
        state = nothing
        seen = nothing
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
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
    for (i, m) in enumerate(ctx.metas)
        fillonce!(m.id, Int32(i - 1))
    end
    validategraphdefaults!(root, length(ctx.metas), ctx.limits, ctx.budget,
                           ctx)
    reserve!(ctx.budget, GRAPH_INFO_BYTES)
    info = GraphInfo(ctx.limits, ctx.repaired_names, ctx.repaired_defaults,
                     length(ctx.metas), ctx.namedcount[])
    allocated!(ctx.budget, GRAPH_INFO_BYTES)
    for m in ctx.metas
        fillonce!(m.graph, info)
        isfilled(m.lexemes) || fillonce!(m.lexemes, EMPTY_SCHEMA_LEXEMES)
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
        return s isa NamedSchema ? hash(s.name, UInt(0x5ecc1e)) : UInt(0x1)
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
    h = hash(s.name, hash(:fixed, UInt(0xa7)))
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
    h = hash(s.name, hash(:enum, UInt(0xa7)))
    h = hash(s.symbols.data, hash(s.aliases.data, hash(s.doc, h)))
    return propshash(s.props, defaulthash(s.default, h))
end

function structuralhash(s::RecordSchema, inprogress)
    h = hash(s.name, hash(s.iserror ? :error : :record, UInt(0xa7)))
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
    i = resolutionsearchsortedfirst(partners, ib, budget)
    i <= length(partners) && partners[i] == ib && return true
    n = checked_add(length(partners), 1)
    addresolution!(budget, n)                         # prefix, new entry and suffix writes
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
`max_schema_bytes`. Public operations use `Limits()` unless the caller supplies a different limit set;
`show` explicitly uses the schema's recorded limits so an admitted schema remains displayable.
"""
mutable struct BoundedWriter <: IO
    buf::Vector{UInt8}      # a package-owned charged buffer grown by reserved exact replacement (D02)
    len::Int
    const budget::Budget
    const maxbytes::Int
    const limit::Symbol
    const credit::Bool
end

function BoundedWriter(budget::Budget, maxbytes::Int; limit::Symbol=:max_schema_bytes,
                       credit::Bool=true)
    cap = min(256, maxbytes)
    charge = bytesbytes(cap)
    reserve!(budget, charge)
    try
        buf = Vector{UInt8}(undef, cap)
        allocated!(budget, charge)
        return BoundedWriter(buf, 0, budget, maxbytes, limit, credit)
    catch
        unreserve!(budget, charge)
        rethrow()
    end
end

function boundedgrow!(w::BoundedWriter, n::Int)
    written = checked_add(w.len, n)
    written <= w.maxbytes ||
        throw(LimitError(w.limit, written, w.maxbytes, w.limit, :encode))
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
    w.credit && addinput!(w.budget, n)                 # produced operation output is the work denominator
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
function json(s::Schema; pretty::Bool=false, limits::Limits=Limits())
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

function printbudget(::IO)
    return nothing
end

function printbudget(io::BoundedWriter)
    return io.budget
end

function rawmember(io::IO, members::FrozenDict{String,RawMember}, key::String)
    return budgetedget(members, key, nothing, printbudget(io))
end

function printattribute(io::IO, members::FrozenDict{String,RawMember}, key::String,
                        pretty::Bool, level::Int)
    indent(io, pretty, level + 1)
    member = rawmember(io, members, key)
    member === nothing ? escapejson(io, key) : print(io, member.key)
    print(io, pretty ? ": " : ":")
    return member
end

function printrawvalue(io::IO, member)
    member isa RawMember || return false
    isempty(member.value) && return false
    print(io, member.value)
    return true
end

function printprops(io::IO, p::Props, pretty::Bool, level::Int, first::Bool,
                    members::FrozenDict{String,RawMember}=EMPTY_RAW_MEMBERS)
    for (k, v) in p
        first || print(io, ',')
        first = false
        member = printattribute(io, members, k, pretty, level)
        printrawvalue(io, member) || printjson(io, v, pretty, level + 1)
    end
    return first
end

function printschema(io::IO, s::Schema, enclosing::String, seen::Vector{Bool}, pretty::Bool, level::Int)
    if s isa PrimitiveSchema
        members = schemalexemes(s).members
        if isempty(s.props) && isempty(members)
            print(io, '"', kind(s), '"')
        else
            print(io, "{")
            member = printattribute(io, members, "type", pretty, level)
            printrawvalue(io, member) || print(io, '"', kind(s), '"')
            printprops(io, s.props, pretty, level, false, members)
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
        members = schemalexemes(s).members
        childkey = s isa ArraySchema ? "items" : "values"
        print(io, '{')
        member = printattribute(io, members, "type", pretty, level)
        printrawvalue(io, member) || print(io, s isa ArraySchema ? "\"array\"" : "\"map\"")
        print(io, ',')
        printattribute(io, members, childkey, pretty, level)
        printschema(io, s isa ArraySchema ? s.items : s.values, enclosing, seen, pretty, level + 1)
        printprops(io, s.props, pretty, level, false, members)
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
    members = schemalexemes(s).members
    print(io, '{')
    member = printattribute(io, members, "type", pretty, level)
    printrawvalue(io, member) || print(io, '"', kind(s), '"')
    print(io, ',')
    indent(io, pretty, level + 1)
    print(io, pretty ? "\"name\": " : "\"name\":")
    escapejson(io, s.name.name)
    if s.name.namespace != enclosing
        print(io, ',')
        indent(io, pretty, level + 1)
        print(io, pretty ? "\"namespace\": " : "\"namespace\":")
        escapejson(io, s.name.namespace)
    end
    if rawmember(io, members, "aliases") !== nothing || !isempty(s.rawaliases)
        print(io, ',')
        member = printattribute(io, members, "aliases", pretty, level)
        if !printrawvalue(io, member)
            print(io, '[')
            for (i, a) in enumerate(s.rawaliases)
                i > 1 && print(io, ',')
                escapejson(io, a)
            end
            print(io, ']')
        end
    end
    if s isa Union{RecordSchema,EnumSchema} && s.doc !== nothing
        print(io, ',')
        member = printattribute(io, members, "doc", pretty, level)
        printrawvalue(io, member) || escapejson(io, s.doc)
    end
    return members
end

function printnamed(io::IO, s::FixedSchema, enclosing::String, seen, pretty::Bool, level::Int)
    members = printnamedheader(io, s, enclosing, pretty, level)
    print(io, ',')
    member = printattribute(io, members, "size", pretty, level)
    printrawvalue(io, member) || print(io, s.size)
    printprops(io, s.props, pretty, level, false, members)
    indent(io, pretty, level)
    print(io, '}')
    return nothing
end

function printnamed(io::IO, s::EnumSchema, enclosing::String, seen, pretty::Bool, level::Int)
    members = printnamedheader(io, s, enclosing, pretty, level)
    print(io, ',')
    member = printattribute(io, members, "symbols", pretty, level)
    if !printrawvalue(io, member)
        print(io, '[')
        for (i, sym) in enumerate(s.symbols)
            i > 1 && print(io, ',')
            escapejson(io, sym)
        end
        print(io, ']')
    end
    if s.default isa DefaultValue
        print(io, ',')
        member = printattribute(io, members, "default", pretty, level)
        printrawvalue(io, member) || printdefault(io, s.default)
    end
    printprops(io, s.props, pretty, level, false, members)
    indent(io, pretty, level)
    print(io, '}')
    return nothing
end

function printdefault(io::IO, d::DefaultValue)
    return isempty(d.span) ? printjson(io, d.json, false, 0) : print(io, d.span)
end

function printnamed(io::IO, s::RecordSchema, enclosing::String, seen, pretty::Bool, level::Int)
    lexemes = schemalexemes(s)
    members = printnamedheader(io, s, enclosing, pretty, level)
    print(io, ',')
    printattribute(io, members, "fields", pretty, level)
    print(io, '[')
    ns = s.name.namespace
    for (i, f) in enumerate(s.fields)
        fieldmembers = i <= length(lexemes.fields) ? lexemes.fields[i] : EMPTY_RAW_MEMBERS
        i > 1 && print(io, ',')
        indent(io, pretty, level + 2)
        print(io, '{')
        member = printattribute(io, fieldmembers, "name", pretty, level + 2)
        printrawvalue(io, member) || escapejson(io, f.name)
        print(io, ',')
        printattribute(io, fieldmembers, "type", pretty, level + 2)
        printschema(io, f.schema, ns, seen, pretty, level + 3)
        if f.doc !== nothing
            print(io, ',')
            member = printattribute(io, fieldmembers, "doc", pretty, level + 2)
            printrawvalue(io, member) || escapejson(io, f.doc)
        end
        if f.default isa DefaultValue
            print(io, ',')
            member = printattribute(io, fieldmembers, "default", pretty, level + 2)
            printrawvalue(io, member) || printdefault(io, f.default)
        end
        if rawmember(io, fieldmembers, "order") !== nothing || f.order != :ascending
            print(io, ',')
            member = printattribute(io, fieldmembers, "order", pretty, level + 2)
            printrawvalue(io, member) || print(io, '"', f.order, '"')
        end
        if rawmember(io, fieldmembers, "aliases") !== nothing || !isempty(f.aliases)
            print(io, ',')
            member = printattribute(io, fieldmembers, "aliases", pretty, level + 2)
            if !printrawvalue(io, member)
                print(io, '[')
                for (j, a) in enumerate(f.aliases)
                    j > 1 && print(io, ',')
                    escapejson(io, a)
                end
                print(io, ']')
            end
        end
        printprops(io, f.props, pretty, level + 2, false, fieldmembers)
        indent(io, pretty, level + 2)
        print(io, '}')
    end
    length(s.fields) > 0 && indent(io, pretty, level + 1)
    print(io, ']')
    printprops(io, s.props, pretty, level, false, members)
    indent(io, pretty, level)
    print(io, '}')
    return nothing
end

function Base.show(io::IO, s::Schema)
    return print(io, "Avro.Schema(", json(s; limits=graphlimits(s)), ")")
end

function Base.show(io::IO, ::MIME"text/plain", s::Schema)
    print(io, "Avro.Schema ", json(s; pretty=true, limits=graphlimits(s)))
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
    meta = publicmeta()
    if haslogical
        k = T === IntSchema ? :int : T === LongSchema ? :long : T === BytesSchema ? :bytes : :string
        s = publicnode(T(evaluatelogical(k, 0, p), p, meta))
        return finalizepublic!(s, limits, 1, 0)
    end
    s = publicnode(T(p, meta))
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

function budgetedjsoninsert!(dict::FrozenDict{String,Any}, key::String, value, budget::Budget)
    box = isbits(value) ? boxbytes(typeof(value)) : 0
    box > 0 && reserve!(budget, box)
    budgetedinsert!(dict, key, value, budget)
    box > 0 && allocated!(budget, box)
    return dict
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
        ks = ownedstringcopy(k, budget)
        ks in structural && throw(ArgumentError("`props` key \"$ks\" collides with a structural attribute emitted by this constructor"))
        logical !== nothing && ks in ("logicalType", "precision", "scale") &&
            throw(ArgumentError("`props` key \"$ks\" is synthesised by `logical=`"))
        budgetedhaskey(p, ks, budget) && throw(ArgumentError("duplicate `props` key \"$ks\""))
        budgetedjsoninsert!(p, ks, tojsonvalue(v, budget), budget) # unsized inputs grow by exact replacement
    end
    if logical !== nothing
        budgetedjsoninsert!(p, "logicalType", logicalname(logical), budget)
        if logical isa DecimalLogical
            budgetedjsoninsert!(p, "precision", Int64(logical.precision), budget)
            budgetedjsoninsert!(p, "scale", Int64(logical.scale), budget)
        end
    end
    return freeze!(p)
end

function tojsonvalue(x, b::Budget, depth::Int=1)
    depth <= b.limits.max_schema_depth ||
        throw(LimitError(:max_schema_depth, depth, b.limits.max_schema_depth,
                         :max_schema_depth, b.direction))
    countvalues!(b)
    return tojsonvalueimpl(x, b, depth)
end

function tojsonvalueimpl(x, b::Budget, ::Int)
    return tojsonvalueimpl(x, b)
end

struct UnsupportedJSONValueError <: Exception
    type::Type
end

function tojsonvalueimpl(x, ::Budget)
    throw(UnsupportedJSONValueError(typeof(x)))
end

function tojsonvalueimpl(x::Union{Nothing,Bool,Int64}, ::Budget)
    return x
end

function ownedstringcopy(x::AbstractString, budget::Budget)
    n = sizeof(x)
    peak = bytesbytes(n) + stringbytes(0)
    reserve!(budget, peak)
    bytes = Vector{UInt8}(undef, n)
    allocated!(budget, bytesbytes(n))
    copyto!(bytes, 1, codeunits(x), 1, n)
    text = String(bytes)                              # takes ownership of the byte vector
    if n == 0
        unreserve!(budget, stringbytes(0))
        retain!(budget, stringbytes(0))
    else
        allocated!(budget, stringbytes(0))
    end
    release!(budget, bytesbytes(0))
    return text
end

function ownedstringcopy(x::AbstractString, from::Int, n::Int, budget::Budget)
    peak = bytesbytes(n) + stringbytes(0)
    reserve!(budget, peak)
    bytes = Vector{UInt8}(undef, n)
    allocated!(budget, bytesbytes(n))
    copyto!(bytes, 1, codeunits(x), from, n)
    text = String(bytes)
    if n == 0
        unreserve!(budget, stringbytes(0))
        retain!(budget, stringbytes(0))
    else
        allocated!(budget, stringbytes(0))
    end
    release!(budget, bytesbytes(0))
    return text
end

"Convert an owned byte vector to a String by transferring its charged payload."
function takeownedstring!(bytes::Vector{UInt8}, budget::Budget)
    shell = stringbytes(0)
    reserve!(budget, shell)
    text = String(bytes)
    if isempty(text)
        unreserve!(budget, shell)
        retain!(budget, shell)
    else
        allocated!(budget, shell)
    end
    release!(budget, bytesbytes(0))
    return text
end

function ownedstringcopy(x::Symbol, budget::Budget)
    charge = stringbytes(sizeof(x))
    reserve!(budget, charge)
    text = String(x)
    allocated!(budget, charge)
    return text
end

function tojsonvalueimpl(x::String, b::Budget)
    return ownedstringcopy(x, b)
end

function tojsonvalueimpl(x::JSONNumber, b::Budget)
    reserve!(b, 32)
    copy = JSONNumber(ownedstringcopy(x.text, b))
    allocated!(b, 32)
    return copy
end

function tojsonvalueimpl(x::JSONArray, b::Budget, depth::Int)
    return copyjsonarray(x.items, b, depth)
end

function tojsonvalueimpl(x::JSONObject, b::Budget, depth::Int)
    n = length(x)
    n == 0 && return EMPTY_JSON_OBJECT
    slots = frozendictshell() + frozenvectorshell() + 24 * n + 64
    reserve!(b, slots)
    members = emptywithcapacity(FrozenDict{String,Any}, n)
    order = emptywithcapacity(FrozenVector{String}, n)
    allocated!(b, slots)
    for key in x.order
        copy = ownedstringcopy(key, b)
        value = budgetedgetindex(x.members, key, b)
        budgetedjsoninsert!(members, copy, tojsonvalue(value, b, depth + 1), b)
        push!(order, copy)
    end
    return JSONObject(freeze!(members), freeze!(order))
end

function tojsonvalueimpl(x::Integer, ::Budget)
    return Int64(x)
end

function tojsonvalueimpl(x::AbstractFloat, b::Budget)
    isfinite(x) || return ownedstringcopy(isnan(x) ? "NaN" :
                                          (x > 0 ? "Infinity" : "-Infinity"), b)
    maximum = stringbytes(32)                         # Float64's shortest round-trip text is at most 24 bytes
    reserve!(b, checked_add(maximum, 32))
    text = repr(Float64(x))
    actual = stringbytes(sizeof(text))
    actual <= maximum || throw(ArgumentError("internal error: Float64 representation exceeds 32 bytes"))
    allocated!(b, actual)
    unreserve!(b, maximum - actual)
    number = JSONNumber(text)
    allocated!(b, 32)
    return number
end

function tojsonvalueimpl(x::AbstractString, b::Budget)
    return ownedstringcopy(x, b)
end

function tojsonvalueimpl(x::Symbol, b::Budget)
    return ownedstringcopy(x, b)
end

function tojsonvalueimpl(x::AbstractVector, b::Budget, depth::Int)
    return copyjsonarray(x, b, depth)
end

function copyjsonarray(x::AbstractVector, b::Budget, depth::Int)
    n = length(x)
    n == 0 && return EMPTY_JSON_ARRAY
    slots = 64 + 24 + vectorbytes(Any, n)
    reserve!(b, slots)                                 # shell, exact capacity and wrapper (§4.4)
    v = emptywithcapacity(FrozenVector{Any}, n)
    allocated!(b, slots)
    for e in x
        value = tojsonvalue(e, b, depth + 1)
        box = isbits(value) ? boxbytes(typeof(value)) : 0
        box > 0 && reserve!(b, box)
        push!(v, value)
        box > 0 && allocated!(b, box)
    end
    return JSONArray(freeze!(v))
end

function tojsonvalueimpl(x::Union{AbstractDict,NamedTuple}, b::Budget, depth::Int)
    n = length(x)
    n == 0 && return EMPTY_JSON_OBJECT
    slots = frozendictshell() + frozenvectorshell() + 24 * n + 64
    reserve!(b, slots)                                 # shells, exact slot capacity and wrapper (§4.4)
    m = emptywithcapacity(FrozenDict{String,Any}, n)
    order = emptywithcapacity(FrozenVector{String}, n)
    allocated!(b, slots)
    for (k, v) in (x isa NamedTuple ? pairs(x) : x)
        k isa Union{AbstractString,Symbol} || throw(ArgumentError("JSON object keys must be strings or symbols"))
        ks = ownedstringcopy(k, b)
        budgetedhaskey(m, ks, b) && throw(ArgumentError("duplicate key \"$ks\""))
        budgetedjsoninsert!(m, ks, tojsonvalue(v, b, depth + 1), b)
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

"Run public schema construction inside an operation budget that the caller already owns."
function withconstructionbudget(f, budget::Budget)
    existing = get(task_local_storage(), :avro_construction_budget, nothing)
    existing === nothing || return f(existing::Budget)
    task_local_storage(:avro_construction_budget, budget)
    try
        return f(budget)
    finally
        task_local_storage(:avro_construction_budget, nothing)
    end
end

"The open construction budget of the current public constructor call (an internal invariant)."
function constructionbudget()
    b = get(task_local_storage(), :avro_construction_budget, nothing)
    b === nothing && throw(ArgumentError("internal error: no construction budget is open"))
    return b::Budget
end

function publicmeta()
    budget = constructionbudget()
    reserve!(budget, NODE_META_BYTES + NODE_SHELL_BYTES)
    meta = NodeMeta()
    allocated!(budget, NODE_META_BYTES)
    return meta
end

function publicnode(schema::Schema)
    allocated!(constructionbudget(), NODE_SHELL_BYTES)
    return schema
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
            releaseownedmemo!(constructionbudget(), memo::FrozenDict{String,Tuple{Schema,Schema}})
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
        walkstate = vectorbytes(NodeMeta, 16) + 2 * frozendictshell() + 64
        reserve!(budget, walkstate)                    # the walk's memo storage and table shells (§4.4)
        walkmetas = Vector{NodeMeta}(undef, 16)
        resize!(walkmetas, 0)
        walk = MetaWalk(walkmetas, 16)
        namedtypes = FrozenDict{String,Schema}()
        identities = FrozenDict{String,Schema}()
        allocated!(budget, walkstate)
        collectmetas!(s, walk, namedtypes, identities, limits, budget, 1)
        metas = walk.metas
        length(namedtypes) <= limits.max_named_types ||
            throw(LimitError(:max_named_types, length(namedtypes), limits.max_named_types,
                             :max_named_types, budget.direction))
        validategraphdefaults!(s, length(metas), limits, budget)
        reserve!(budget, GRAPH_INFO_BYTES)
        info = GraphInfo(limits, false, false, length(metas), length(namedtypes))
        allocated!(budget, GRAPH_INFO_BYTES)
        for m in metas
            fillonce!(m.graph, info)
            isfilled(m.lexemes) || fillonce!(m.lexemes, EMPTY_SCHEMA_LEXEMES)
        end
        checkpublicprint!(s, limits, budget)
        computehashes!(s, metas)
        namekeys = 0
        for key in namedtypes.keys
            namekeys = checked_add(namekeys, stringbytes(sizeof(key)))
        end
        release!(budget, vectorbytes(NodeMeta, walk.cap) + frozendictbytes(namedtypes) +
                         frozendictbytes(identities) + namekeys + 64)
        return s
    end
end

"Validate an already-finalized schema graph against a new caller-supplied limit set."
function checkexistinggraph!(s::Schema, limits::Limits, budget::Budget)
    info = graphinfo(s)
    info.nodes <= limits.max_schema_nodes ||
        throw(LimitError(:max_schema_nodes, info.nodes, limits.max_schema_nodes, :max_schema_nodes, budget.direction))
    info.namedtypes <= limits.max_named_types ||
        throw(LimitError(:max_named_types, info.namedtypes, limits.max_named_types, :max_named_types, budget.direction))
    charge = vectorbytes(Bool, info.nodes)
    reserve!(budget, charge)
    seen = Vector{Bool}(undef, info.nodes)
    allocated!(budget, charge)
    fill!(seen, false)
    try
        checkexistingnode!(s, seen, limits, budget, 1)
    finally
        release!(budget, charge)
    end
    checkpublicprint!(s, limits, budget)
    return s
end

function checkexistingnode!(s::Schema, seen::Vector{Bool}, limits::Limits, budget::Budget, depth::Int)
    depth <= limits.max_schema_depth ||
        throw(LimitError(:max_schema_depth, depth, limits.max_schema_depth, :max_schema_depth, budget.direction))
    index = Int(nodeid(s)) + 1
    seen[index] && return nothing
    seen[index] = true
    countvalues!(budget)
    s isa NamedSchema && checkgraphnames(s, limits, budget.direction)
    if s isa ArraySchema
        checkexistingnode!(s.items, seen, limits, budget, depth + 1)
    elseif s isa MapSchema
        checkexistingnode!(s.values, seen, limits, budget, depth + 1)
    elseif s isa UnionSchema
        length(s.branches) <= limits.max_union_branches ||
            throw(LimitError(:max_union_branches, length(s.branches), limits.max_union_branches,
                             :max_union_branches, budget.direction))
        for branch in s.branches
            checkexistingnode!(branch, seen, limits, budget, depth + 1)
        end
    elseif s isa RecordSchema
        length(s.fields) <= limits.max_fields ||
            throw(LimitError(:max_fields, length(s.fields), limits.max_fields, :max_fields, budget.direction))
        for field in s.fields
            checkgraphnamebytes(field.name, limits, budget.direction)
            for alias in field.aliases
                checkgraphnamebytes(alias, limits, budget.direction)
            end
            checkexistingnode!(field.schema, seen, limits, budget, depth + 1)
        end
    elseif s isa EnumSchema
        length(s.symbols) <= limits.max_enum_symbols ||
            throw(LimitError(:max_enum_symbols, length(s.symbols), limits.max_enum_symbols,
                             :max_enum_symbols, budget.direction))
        for symbol in s.symbols
            checkgraphnamebytes(symbol, limits, budget.direction)
        end
    end
    return nothing
end

"Charge and bound the exact schema text retained properties and defaults will produce."
function checkpublicprint!(s::Schema, limits::Limits, budget::Budget)
    writer = BoundedWriter(budget, limits.max_schema_bytes)
    seen = schemaseen(s, budget)
    try
        printschema(writer, s, "", seen, false, 0)
        errfn = (message, position) ->
            throw(ArgumentError("internal error: public schema produced invalid JSON at byte $position: $message"))
        limitfn = (limit, observed, value) ->
            throw(LimitError(limit, observed, value, limit, budget.direction))
        prescan(writer.buf, limits.max_schema_bytes, limits.max_schema_depth, errfn;
                limitfn=limitfn, bytelimit=:max_schema_bytes,
                depthlimit=:max_schema_depth, nbytes=writer.len)
    finally
        releaseseen!(budget, seen)
    end
    return nothing
end

function checkgraphnamebytes(name::AbstractString, limits::Limits, direction::Symbol)
    sizeof(name) <= limits.max_name_bytes ||
        throw(LimitError(:max_name_bytes, sizeof(name), limits.max_name_bytes,
                         :max_name_bytes, direction))
    return nothing
end

function checkgraphnames(s::NamedSchema, limits::Limits, direction::Symbol)
    fullnamesize(s.name) <= limits.max_name_bytes ||
        throw(LimitError(:max_name_bytes, fullnamesize(s.name), limits.max_name_bytes,
                         :max_name_bytes, direction))
    for a in s.aliases
        checkgraphnamebytes(a, limits, direction)
    end
    return nothing
end

"The finalisation walk's growable node memo (§4.4 exact replacement across recursive frames)."
mutable struct MetaWalk
    metas::Vector{NodeMeta}
    cap::Int
end

function collectmetas!(s::Schema, walk::MetaWalk, namedtypes::FrozenDict{String,Schema},
                       identities::FrozenDict{String,Schema},
                       limits::Limits, budget::Budget, depth::Int)
    depth <= limits.max_schema_depth ||
        throw(LimitError(:max_schema_depth, depth, limits.max_schema_depth,
                         :max_schema_depth, budget.direction))
    countvalues!(budget)
    isfilled(s.meta.id) && return walk
    length(walk.metas) < limits.max_schema_nodes ||
        throw(LimitError(:max_schema_nodes, length(walk.metas) + 1, limits.max_schema_nodes,
                         :max_schema_nodes, budget.direction))
    fillonce!(s.meta.id, Int32(length(walk.metas)))
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
        checkgraphnames(s, limits, budget.direction)
        if budgetedhaskey(namedtypes, s.name, budget)
            throw(ArgumentError("a named schema is defined more than once"))
        end
        budgetedhaskey(identities, s.name, budget) &&
            throw(ArgumentError("a named schema name or alias is used by more than one type"))
        for alias in s.aliases
            budgetedhaskey(identities, alias, budget) &&
                throw(ArgumentError("named schema name or alias \"$alias\" is used by more than one type"))
        end
        fullkey = ownedfullname(s.name, budget)
        budgetedinsert!(namedtypes, fullkey, s, budget)
        budgetedinsert!(identities, fullkey, s, budget)
        for alias in s.aliases
            budgetedinsert!(identities, alias, s, budget)
        end
    end
    if s isa ArraySchema
        collectmetas!(s.items, walk, namedtypes, identities, limits, budget, depth + 1)
    elseif s isa MapSchema
        collectmetas!(s.values, walk, namedtypes, identities, limits, budget, depth + 1)
    elseif s isa UnionSchema
        length(s.branches) <= limits.max_union_branches ||
            throw(LimitError(:max_union_branches, length(s.branches), limits.max_union_branches,
                             :max_union_branches, budget.direction))
        foreach(b -> collectmetas!(b, walk, namedtypes, identities, limits, budget, depth + 1), s.branches)
    elseif s isa RecordSchema
        length(s.fields) <= limits.max_fields ||
            throw(LimitError(:max_fields, length(s.fields), limits.max_fields,
                             :max_fields, budget.direction))
        for f in s.fields
            checkgraphnamebytes(f.name, limits, budget.direction)
            for a in f.aliases
                checkgraphnamebytes(a, limits, budget.direction)
            end
            collectmetas!(f.schema, walk, namedtypes, identities, limits, budget, depth + 1)
        end
    elseif s isa EnumSchema
        length(s.symbols) <= limits.max_enum_symbols ||
            throw(LimitError(:max_enum_symbols, length(s.symbols), limits.max_enum_symbols,
                             :max_enum_symbols, budget.direction))
        for sym in s.symbols
            checkgraphnamebytes(sym, limits, budget.direction)
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
    return nothing
end

function ownedfullname(f::FullName, budget::Budget)
    isempty(f.namespace) && return ownedstringcopy(f.name, budget)
    n = checked_add(checked_add(sizeof(f.namespace), 1), sizeof(f.name))
    charge = stringbytes(n)
    reserve!(budget, charge)
    out = string(f.namespace, ".", f.name)
    allocated!(budget, charge)
    return out
end

function ownednormalizedalias(alias::String, namespace::String, budget::Budget)
    (findlast('.', alias) !== nothing || isempty(namespace)) &&
        return ownedstringcopy(alias, budget)
    n = checked_add(checked_add(sizeof(namespace), 1), sizeof(alias))
    charge = stringbytes(n)
    reserve!(budget, charge)
    out = string(namespace, ".", alias)
    allocated!(budget, charge)
    return out
end

function publicnamed(name::AbstractString, namespace::AbstractString, aliases, structural, propsin, logical)
    b = constructionbudget()
    sizeof(name) <= b.limits.max_name_bytes ||
        throw(LimitError(:max_name_bytes, sizeof(name), b.limits.max_name_bytes, :max_name_bytes, b.direction))
    dotted = findlast('.', name) !== nothing
    dotted || sizeof(namespace) <= b.limits.max_name_bytes ||
        throw(LimitError(:max_name_bytes, sizeof(namespace), b.limits.max_name_bytes, :max_name_bytes, b.direction))
    dot = findlast('.', name)
    if dot === nothing
        checkpublicname(name, "name")
        isvalidnamespace(namespace) || throw(ArgumentError("invalid namespace \"$namespace\""))
        n = ownedstringcopy(name, b)
        ns = ownedstringcopy(namespace, b)
    else
        namefirst = nextind(name, dot)
        namepart = SubString(name, namefirst, lastindex(name))
        namespacepart = SubString(name, firstindex(name), prevind(name, dot))
        checkpublicname(namepart, "name")
        isvalidnamespace(namespacepart) || throw(ArgumentError("invalid namespace \"$namespacepart\""))
        n = ownedstringcopy(name, namefirst, sizeof(name) - dot, b)
        ns = ownedstringcopy(name, firstindex(name), dot - 1, b)
    end
    full = FullName(n, ns)
    isreservedfullname(full) && throw(ArgumentError("\"$n\" is a primitive type name and cannot be redefined in the null namespace"))
    raw = BuildBuf{String}(b, 2)
    norm = BuildBuf{String}(b, 2)
    reserve!(b, frozendictshell())
    seen = FrozenDict{String,Bool}()
    allocated!(b, frozendictshell())
    canonical = ownedfullname(full, b)
    canonicalcharge = stringbytes(sizeof(canonical))
    try
        for a in aliases
            a isa Union{AbstractString,Symbol} || throw(ArgumentError("aliases must be strings"))
            sizeof(a) <= b.limits.max_name_bytes ||
                throw(LimitError(:max_name_bytes, sizeof(a), b.limits.max_name_bytes, :max_name_bytes, b.direction))
            sa = ownedstringcopy(a, b)
            dot = findlast('.', sa)
            normalizedsize = dot === nothing && !isempty(full.namespace) ?
                             checked_add(checked_add(sizeof(full.namespace), 1), sizeof(sa)) : sizeof(sa)
            normalizedsize <= b.limits.max_name_bytes ||
                throw(LimitError(:max_name_bytes, normalizedsize, b.limits.max_name_bytes,
                                 :max_name_bytes, b.direction))
            na = ownednormalizedalias(sa, full.namespace, b)
            push!(raw, b, sa)
            if na == canonical || budgetedhaskey(seen, na, b)
                release!(b, stringbytes(sizeof(na)))
                continue
            end
            budgetedinsert!(seen, na, true, b)
            push!(norm, b, na)
        end
    finally
        release!(b, frozendictbytes(seen) + canonicalcharge)
    end
    wrappers = 48
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
        meta = publicmeta()
        s = publicnode(FixedSchema(full, norm, raw, Int(size),
                                  evaluatelogical(:fixed, Int(size), p), p, meta))
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
        symsbuf = BuildBuf{String}(b, 4)
        for x in symbols
            x isa Union{AbstractString,Symbol} || throw(ArgumentError("enum symbols must be strings"))
            symsbuf.len < limits.max_enum_symbols || throw(LimitError(:max_enum_symbols, symsbuf.len + 1, limits.max_enum_symbols, :max_enum_symbols, :encode))
            checkgraphnamebytes(x, limits, b.direction)
            sym = ownedstringcopy(x, b)
            push!(symsbuf, b, sym)
        end
        syms = finishbuild!(symsbuf, b)
        idxbytes = 48 + vectorbytes(String, length(syms)) + vectorbytes(Int, length(syms))
        reserve!(b, idxbytes)                          # the index shell and exact capacity (§4.4)
        index = emptywithcapacity(FrozenDict{String,Int}, length(syms))
        allocated!(b, idxbytes)
        for (i, sym) in enumerate(syms)
            checkpublicname(sym, "enum symbol")
            budgetedhaskey(index, sym, b) && throw(ArgumentError("duplicate enum symbol \"$sym\""))
            budgetedinsert!(index, sym, i, b)
        end
        d = nodefault
        if !(default isa NoDefault)
            default isa AbstractString ||
                throw(ArgumentError("enum default must be one of the symbols"))
            checkgraphnamebytes(default, limits, b.direction)
            ds = ownedstringcopy(default, b)
            budgetedhaskey(index, ds, b) ||
                throw(ArgumentError("enum default must be one of the symbols"))
            jw = BoundedWriter(b, limits.max_schema_bytes)
            escapejson(jw, ds)
            d = DefaultValue(ds, 0, boundedtake!(jw), budgetedgetindex(index, ds, b), true)
        end
        doccopy = doc === nothing ? nothing : ownedstringcopy(doc, b)
        reserve!(b, 24)                                # the symbol wrapper, made next
        symbols = freeze!(FrozenVector{String}(syms, false))
        allocated!(b, 24)
        meta = publicmeta()
        s = publicnode(EnumSchema(full, norm, raw, doccopy, symbols, d,
                                  freeze!(index), p, meta))
        return finalizepublic!(s, limits, 1, 1)
    end
end

"""
    Avro.ArraySchema(items; props=(;), limits=Limits()) / Avro.MapSchema(values; props=(;), limits=Limits())
"""
function ArraySchema(items::Schema; props=(;), limits::Limits=Limits())
    return withconstruction(limits) do _
        s = withbuilder() do
            child = importchild(items)
            attributes = makeprops(props, SCHEMA_GRAMMAR[:array], nothing)
            return publicnode(ArraySchema(child, attributes, publicmeta()))
        end
        return finalizepublic!(s, limits, 0, 0)
    end
end

function MapSchema(values::Schema; props=(;), limits::Limits=Limits())
    return withconstruction(limits) do _
        s = withbuilder() do
            child = importchild(values)
            attributes = makeprops(props, SCHEMA_GRAMMAR[:map], nothing)
            return publicnode(MapSchema(child, attributes, publicmeta()))
        end
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
        reserve!(b, frozendictshell())
        seen = FrozenDict{BranchIdentity,Int}()
        allocated!(b, frozendictshell())
        try
            withbuilder() do
                for branch in branches
                    bs.len < limits.max_union_branches ||
                        throw(LimitError(:max_union_branches, bs.len + 1, limits.max_union_branches,
                                         :max_union_branches, :encode))
                    branch isa Schema || throw(ArgumentError("union branches must be schemas"))
                    branch isa UnionSchema && throw(ArgumentError("unions may not immediately contain other unions"))
                    ident = branchidentity(branch)
                    budgetedhaskey(seen, ident, b) &&
                        throw(ArgumentError("duplicate union branch $(branchidentitylabel(branch))"))
                    budgetedinsert!(seen, ident, bs.len + 1, b)
                    push!(bs, b, importchild(branch))
                end
            end
        finally
            release!(b, frozendictbytes(seen))
        end
        bs.len <= limits.max_union_branches || throw(LimitError(:max_union_branches, bs.len, limits.max_union_branches, :max_union_branches, :encode))
        data = finishbuild!(bs, b)
        reserve!(b, 24)
        frozen = freeze!(FrozenVector{Schema}(data, false))
        allocated!(b, 24)
        schema = publicnode(UnionSchema(frozen, publicmeta()))
        return finalizepublic!(schema, limits, 0, 0)
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
        checkgraphnamebytes(name, b.limits, b.direction)
        checkpublicname(name, "field name")
        order in (:ascending, :descending, :ignore) || throw(ArgumentError("order must be :ascending, :descending or :ignore"))
        als = BuildBuf{String}(b, 2)
        reserve!(b, frozendictshell())
        seen = FrozenDict{String,Bool}()
        allocated!(b, frozendictshell())
        try
            for a in aliases
                a isa Union{AbstractString,Symbol} || throw(ArgumentError("aliases must be strings"))
                checkgraphnamebytes(a, b.limits, b.direction)
                sa = ownedstringcopy(a, b)
                if sa == name || budgetedhaskey(seen, sa, b)
                    release!(b, stringbytes(sizeof(sa)))
                    continue
                end
                budgetedinsert!(seen, sa, true, b)
                push!(als, b, sa)
            end
        finally
            release!(b, frozendictbytes(seen))
        end
        d = nodefault
        if !(default isa NoDefault)
            j = tojsonvalue(default === missing ? nothing : default, b)
            ok, branch = validatedefault(schema, j, limits.max_depth, 1, b)
            ok || throw(ArgumentError("default value for field \"$name\" does not match its schema"))
            jw = BoundedWriter(b, limits.max_schema_bytes)
            printjson(jw, j, false, 0)
            d = DefaultValue(j, branch, boundedtake!(jw), 0, true)
        end
        namecopy = ownedstringcopy(name, b)
        doccopy = doc === nothing ? nothing : ownedstringcopy(doc, b)
        fbytes = 24 + 128
        reserve!(b, fbytes)                            # the frozen wrapper and Field shell, made next
        f = Field(namecopy, schema, doccopy, d, order,
                  freeze!(FrozenVector{String}(finishbuild!(als, b), false)),
                  makeprops(props, FIELD_GRAMMAR, nothing))
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
    recbytes = frozenvectorshell() + frozendictshell()
    reserve!(b, recbytes)                              # the field-vector and index shells, made next
    fields = FrozenVector{Field}()
    index = FrozenDict{String,Int}()
    allocated!(b, recbytes)
    doccopy = doc === nothing ? nothing : ownedstringcopy(doc, b)
    rec = publicnode(RecordSchema(full, norm, raw, doccopy, iserror, p,
                                  fields, index, publicmeta()))
    withbuilder() do
        fs = f(rec)
        length(fs) <= limits.max_fields || throw(LimitError(:max_fields, length(fs), limits.max_fields, :max_fields, :encode))
        for (i, fld) in enumerate(fs)
            fld isa Field || throw(ArgumentError("fields must be Avro.Field values"))
            budgetedhaskey(index, fld.name, b) &&
                throw(ArgumentError("duplicate field name \"$(fld.name)\""))
            field = copyfield(fld, importchild(fld.schema), b, 3)
            budgetedpush!(fields, field, b)
            budgetedinsert!(index, field.name, i, b)
        end
    end
    reserve!(b, frozendictshell())
    aliasowners = FrozenDict{String,Int}()
    allocated!(b, frozendictshell())
    try
        for (i, field) in enumerate(fields), alias in field.aliases
            budgetedhaskey(index, alias, b) &&
                throw(ArgumentError("field alias \"$alias\" collides with a field name"))
            budgetedhaskey(aliasowners, alias, b) &&
                throw(ArgumentError("field alias \"$alias\" is declared by two fields"))
            budgetedinsert!(aliasowners, alias, i, b)
        end
    finally
        release!(b, frozendictbytes(aliasowners))
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
    b = constructionbudget()
    checkexistinggraph!(s, b.limits, b)
    memo = importmemo()
    memo !== nothing && return deepcopyschema(s, memo)
    reserve!(b, frozendictshell())
    localmemo = FrozenDict{String,Tuple{Schema,Schema}}()
    allocated!(b, frozendictshell())
    try
        return deepcopyschema(s, localmemo)
    finally
        releaseownedmemo!(b, localmemo)
    end
end

function memocopy(s::Schema, memo::FrozenDict{String,Schema})
    return budgetedgetindex(memo, s.name, constructionbudget())
end

function memocopy(s::Schema, memo::FrozenDict{String,Tuple{Schema,Schema}})
    source, copy = budgetedgetindex(memo, s.name, constructionbudget())
    source === s || throw(ArgumentError("a named schema is defined by more than one child schema"))
    return copy
end

function remembercopy!(memo::FrozenDict{String,Schema}, s::Schema, copy::Schema)
    key = ownedfullname(s.name, constructionbudget())
    budgetedinsert!(memo, key, copy, constructionbudget())
    return copy
end

function remembercopy!(memo::FrozenDict{String,Tuple{Schema,Schema}}, s::Schema, copy::Schema)
    key = ownedfullname(s.name, constructionbudget())
    budgetedinsert!(memo, key, (s, copy), constructionbudget())
    return copy
end

function releaseownedmemo!(budget::Budget, memo::FrozenDict{String})
    charge = frozendictbytes(memo)
    for key in memo.keys
        charge = checked_add(charge, stringbytes(sizeof(key)))
    end
    release!(budget, charge)
    return nothing
end

function copyjsonvalue(value, budget::Budget, depth::Int)
    depth <= budget.limits.max_schema_depth ||
        throw(LimitError(:max_schema_depth, depth, budget.limits.max_schema_depth,
                         :max_schema_depth, budget.direction))
    countvalues!(budget)
    value === nothing && return nothing
    value isa Union{Bool,Int64,Float64} && return value
    value isa String && return ownedstringcopy(value, budget)
    if value isa JSONNumber
        reserve!(budget, 32)
        copy = JSONNumber(ownedstringcopy(value.text, budget))
        allocated!(budget, 32)
        return copy
    end
    value isa JSONArray && return copyjsonarray(value, budget, depth)
    value isa JSONObject && return copyjsonobject(value, budget, depth)
    throw(ArgumentError("internal error: unsupported frozen JSON value $(typeof(value))"))
end

function assignjsoncopy!(values::Vector{Any}, index::Int, value, budget::Budget)
    box = isbits(value) ? boxbytes(typeof(value)) : 0
    box > 0 && reserve!(budget, box)
    values[index] = value
    box > 0 && allocated!(budget, box)
    return nothing
end

function copyjsonarray(source::JSONArray, budget::Budget, depth::Int)
    n = length(source)
    n == 0 && return EMPTY_JSON_ARRAY
    charge = 64 + 24 + vectorbytes(Any, n)
    reserve!(budget, charge)
    values = Vector{Any}(undef, n)
    copy = JSONArray(FrozenVector{Any}(values, false))
    allocated!(budget, charge)
    for (i, value) in enumerate(source.items)
        assignjsoncopy!(values, i, copyjsonvalue(value, budget, depth + 1), budget)
    end
    freeze!(copy)
    return copy
end

function copyrangevector(source::FrozenVector{JSONMemberSpan}, budget::Budget)
    isempty(source) && return EMPTY_JSON_SPANS
    charge = 24 + vectorbytes(JSONMemberSpan, length(source))
    reserve!(budget, charge)
    values = Base.copy(source.data)
    out = FrozenVector{JSONMemberSpan}(values, true)
    allocated!(budget, charge)
    return out
end

function copyjsonobject(source::JSONObject, budget::Budget, depth::Int)
    n = length(source)
    n == 0 && return EMPTY_JSON_OBJECT
    arrays = vectorbytes(String, n) + vectorbytes(Any, n) + vectorbytes(String, n)
    reserve!(budget, arrays)
    sortedkeys = Vector{String}(undef, n)
    sortedvalues = Vector{Any}(undef, n)
    order = Vector{String}(undef, n)
    allocated!(budget, arrays)
    for (i, key) in enumerate(source.members.keys)
        sortedkeys[i] = ownedstringcopy(key, budget)
        assignjsoncopy!(sortedvalues, i,
            copyjsonvalue(source.members.vals[i], budget, depth + 1), budget)
    end
    for (i, key) in enumerate(source.order)
        index = budgetedkeyindex(source.members, key, budget)
        index == 0 && throw(ArgumentError("internal error: JSON object order contains an unknown key"))
        order[i] = sortedkeys[index]
    end
    ranges = copyrangevector(source.spans, budget)
    wrappers = 64 + 48 + 24
    reserve!(budget, wrappers)
    members = FrozenDict{String,Any}(sortedkeys, sortedvalues, true)
    ordered = FrozenVector{String}(order, true)
    out = JSONObject(members, ordered, ranges)
    allocated!(budget, wrappers)
    return out
end

function copyprops(props::Props, budget::Budget, depth::Int)
    isempty(props) && return EMPTY_PROPS
    n = length(props)
    charge = 48 + vectorbytes(String, n) + vectorbytes(Any, n)
    reserve!(budget, charge)
    copy = emptywithcapacity(Props, n)
    allocated!(budget, charge)
    for (key, value) in props
        copiedkey = ownedstringcopy(key, budget)
        copiedvalue = copyjsonvalue(value, budget, depth)
        budgetedjsoninsert!(copy, copiedkey, copiedvalue, budget)
    end
    return freeze!(copy)
end

function copystringvector(source::FrozenVector{String}, budget::Budget)
    isempty(source) && return EMPTY_STRING_LIST
    n = length(source)
    charge = 24 + vectorbytes(String, n)
    reserve!(budget, charge)
    values = Vector{String}(undef, n)
    copy = FrozenVector{String}(values, false)
    allocated!(budget, charge)
    for (i, value) in enumerate(source)
        values[i] = ownedstringcopy(value, budget)
    end
    return freeze!(copy)
end

function copyfullname(source::FullName, budget::Budget)
    return FullName(ownedstringcopy(source.name, budget),
                    ownedstringcopy(source.namespace, budget))
end

function copydefault(default::Default, budget::Budget, depth::Int)
    default isa DefaultValue || return nodefault
    jsoncopy = copyjsonvalue(default.json, budget, depth)
    spancopy = ownedstringcopy(default.span, budget)
    return DefaultValue(jsoncopy, default.branch, spancopy, default.index, default.valid)
end

function copyrawmembers(source::FrozenDict{String,RawMember}, budget::Budget)
    isempty(source) && return EMPTY_RAW_MEMBERS
    n = length(source)
    charge = 48 + vectorbytes(String, n) + vectorbytes(RawMember, n)
    reserve!(budget, charge)
    copy = emptywithcapacity(FrozenDict{String,RawMember}, n)
    allocated!(budget, charge)
    for (key, member) in source
        copiedkey = ownedstringcopy(key, budget)
        rawkey = ownedstringcopy(member.key, budget)
        rawvalue = isempty(member.value) ? "" : ownedstringcopy(member.value, budget)
        budgetedinsert!(copy, copiedkey, RawMember(rawkey, rawvalue), budget)
    end
    return freeze!(copy)
end

function copyschemalexemes(source::SchemaLexemes, budget::Budget)
    isempty(source.members) && isempty(source.fields) && return EMPTY_SCHEMA_LEXEMES
    members = copyrawmembers(source.members, budget)
    isempty(source.fields) && return SchemaLexemes(members, EMPTY_FIELD_LEXEMES)
    n = length(source.fields)
    charge = 24 + vectorbytes(FrozenDict{String,RawMember}, n)
    reserve!(budget, charge)
    fields = Vector{FrozenDict{String,RawMember}}(undef, n)
    wrapped = FrozenVector{FrozenDict{String,RawMember}}(fields, false)
    allocated!(budget, charge)
    for (i, field) in enumerate(source.fields)
        fields[i] = copyrawmembers(field, budget)
    end
    freeze!(wrapped)
    return SchemaLexemes(members, wrapped)
end

function copyschemalexemes(source::SchemaLexemes, fields::Vector{Int}, budget::Budget)
    isempty(source.members) && isempty(source.fields) && return EMPTY_SCHEMA_LEXEMES
    members = copyrawmembers(source.members, budget)
    isempty(fields) && return SchemaLexemes(members, EMPTY_FIELD_LEXEMES)
    charge = 24 + vectorbytes(FrozenDict{String,RawMember}, length(fields))
    reserve!(budget, charge)
    copiedfields = Vector{FrozenDict{String,RawMember}}(undef, length(fields))
    wrapped = FrozenVector{FrozenDict{String,RawMember}}(copiedfields, false)
    allocated!(budget, charge)
    for (i, field) in enumerate(fields)
        copiedfields[i] = field <= length(source.fields) ?
                          copyrawmembers(source.fields[field], budget) : EMPTY_RAW_MEMBERS
    end
    freeze!(wrapped)
    return SchemaLexemes(members, wrapped)
end

function copiedmeta(source::Schema, budget::Budget)
    meta = publicmeta()
    if isfilled(source.meta.lexemes)
        fillonce!(meta.lexemes, copyschemalexemes(source.meta.lexemes[], budget))
    end
    return meta
end

function copiedmeta(source::Schema, fields::Vector{Int}, budget::Budget)
    meta = publicmeta()
    if isfilled(source.meta.lexemes)
        fillonce!(meta.lexemes, copyschemalexemes(source.meta.lexemes[], fields, budget))
    end
    return meta
end

function copyfield(source::Field, schema::Schema, budget::Budget, depth::Int)
    name = ownedstringcopy(source.name, budget)
    doc = source.doc === nothing ? nothing : ownedstringcopy(source.doc, budget)
    default = copydefault(source.default, budget, depth)
    aliases = copystringvector(source.aliases, budget)
    props = copyprops(source.props, budget, depth)
    reserve!(budget, 128)
    copy = Field(name, schema, doc, default, source.order, aliases, props)
    allocated!(budget, 128)
    return copy
end

function copystringindex(values::FrozenVector{String}, source::FrozenDict{String,Int},
                         budget::Budget)
    n = length(values)
    n == 0 && return freeze!(FrozenDict{String,Int}())
    charge = 48 + vectorbytes(String, n) + vectorbytes(Int, n)
    reserve!(budget, charge)
    index = emptywithcapacity(FrozenDict{String,Int}, n)
    allocated!(budget, charge)
    resize!(index.keys, n)
    resize!(index.vals, n)
    for i in 1:n
        ordinal = source.vals[i]
        index.keys[i] = values[ordinal]
        index.vals[i] = ordinal
    end
    return freeze!(index)
end

function deepcopyschema(s::Schema,
                        memo::Union{FrozenDict{String,Schema},FrozenDict{String,Tuple{Schema,Schema}}},
                        depth::Int=1)
    if s isa NamedSchema
        budgetedhaskey(memo, s.name, constructionbudget()) && return memocopy(s, memo)
    end
    budget = constructionbudget()
    props = s isa UnionSchema ? EMPTY_PROPS : copyprops(s.props, budget, depth + 1)
    meta = copiedmeta(s, budget)
    if s isa NullSchema
        return publicnode(NullSchema(props, meta))
    elseif s isa BooleanSchema
        return publicnode(BooleanSchema(props, meta))
    elseif s isa IntSchema
        return publicnode(IntSchema(evaluatelogical(:int, 0, props), props, meta))
    elseif s isa LongSchema
        return publicnode(LongSchema(evaluatelogical(:long, 0, props), props, meta))
    elseif s isa FloatSchema
        return publicnode(FloatSchema(props, meta))
    elseif s isa DoubleSchema
        return publicnode(DoubleSchema(props, meta))
    elseif s isa BytesSchema
        return publicnode(BytesSchema(evaluatelogical(:bytes, 0, props), props, meta))
    elseif s isa StringSchema
        return publicnode(StringSchema(evaluatelogical(:string, 0, props), props, meta))
    elseif s isa ArraySchema
        child = deepcopyschema(s.items, memo, depth + 1)
        return publicnode(ArraySchema(child, props, meta))
    elseif s isa MapSchema
        child = deepcopyschema(s.values, memo, depth + 1)
        return publicnode(MapSchema(child, props, meta))
    elseif s isa UnionSchema
        n = length(s.branches)
        charge = vectorbytes(Schema, n) + 24
        reserve!(constructionbudget(), charge)
        bs = emptywithcapacity(FrozenVector{Schema}, n)
        allocated!(constructionbudget(), charge)
        for b in s.branches
            budgetedpush!(bs, deepcopyschema(b, memo, depth + 1), constructionbudget())
        end
        return publicnode(UnionSchema(freeze!(bs), meta))
    elseif s isa FixedSchema
        name = copyfullname(s.name, budget)
        aliases = copystringvector(s.aliases, budget)
        rawaliases = copystringvector(s.rawaliases, budget)
        c = publicnode(FixedSchema(name, aliases, rawaliases, s.size,
                                   evaluatelogical(:fixed, s.size, props), props, meta))
        remembercopy!(memo, s, c)
        return c
    elseif s isa EnumSchema
        name = copyfullname(s.name, budget)
        aliases = copystringvector(s.aliases, budget)
        rawaliases = copystringvector(s.rawaliases, budget)
        doc = s.doc === nothing ? nothing : ownedstringcopy(s.doc, budget)
        symbols = copystringvector(s.symbols, budget)
        default = copydefault(s.default, budget, depth + 1)
        index = copystringindex(symbols, s.symbolindex, budget)
        c = publicnode(EnumSchema(name, aliases, rawaliases, doc, symbols,
                                  default, index, props, meta))
        remembercopy!(memo, s, c)
        return c
    else
        name = copyfullname(s.name, budget)
        aliases = copystringvector(s.aliases, budget)
        rawaliases = copystringvector(s.rawaliases, budget)
        doc = s.doc === nothing ? nothing : ownedstringcopy(s.doc, budget)
        n = length(s.fields)
        charge = vectorbytes(Field, n) + 24 + 48 + vectorbytes(String, n) + vectorbytes(Int, n)
        reserve!(constructionbudget(), charge)
        fields = emptywithcapacity(FrozenVector{Field}, n)
        index = emptywithcapacity(FrozenDict{String,Int}, n)
        allocated!(constructionbudget(), charge)
        c = publicnode(RecordSchema(name, aliases, rawaliases, doc, s.iserror,
                                    props, fields, index, meta))
        remembercopy!(memo, s, c)
        for (i, f) in enumerate(s.fields)
            child = deepcopyschema(f.schema, memo, depth + 3)
            field = copyfield(f, child, budget, depth + 3)
            budgetedpush!(fields, field, budget)
        end
        resize!(index.keys, n)
        resize!(index.vals, n)
        for i in 1:n
            position = s.fieldindex.vals[i]
            index.keys[i] = fields[position].name
            index.vals[i] = position
        end
        freeze!(fields)
        freeze!(index)
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
