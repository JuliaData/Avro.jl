# Schema model: immutable schema graphs with dense node ids, the fullname algorithm, the contextual
# attribute grammar, the recursive default rule, printing and structural equality. Nodes are heap
# objects (`Record`, `EnumValue` and `Fixed` carry one reference to theirs).
#
# Functions taking a `Schema` or a parsed JSON value declare it `@nospecialize`, branch on the concrete
# types inside, and walk a node's children in plain loops. A call with an abstract argument then dispatches
# statically, which a program compiled with `juliac --trim` requires.

const Props = JSON.Object{String,Any}   # custom attributes as parsed JSON values (raw number tokens)

"""
    GraphInfo

Per-graph facts filled by finalisation: the repair flags and the node/named-type counts.
"""
mutable struct GraphInfo
    const repaired_names::Bool
    const repaired_defaults::Bool
    const nodes::Int
    const namedtypes::Int
end

"""
    NodeMeta

Node facts set once by finalisation: a dense graph-local `id`, the shared `GraphInfo` and the least
encoded size of a datum.
"""
mutable struct NodeMeta
    id::Int32
    graph::Union{Nothing,GraphInfo}
    minsize::Int
end

function NodeMeta()
    return NodeMeta(Int32(-1), nothing, 0)
end

abstract type Schema end

struct NoDefault end

"""
    Avro.nodefault

The sentinel for "no default" on fields and enums (`nothing`/`missing` as a default mean a JSON `null`).
"""
const nodefault = NoDefault()

"""
    DefaultValue(json, branch, index, valid)

A parsed default: its JSON value, the selected union branch (1-based; 0 when not a union), the enum
symbol index (1-based; 0 otherwise) and whether it validated against the schema (invalid defaults are
kept only under `allow_invalid_defaults=true`).
"""
struct DefaultValue
    json::Any
    branch::Int
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
mutable struct UnionSchema <: Schema; const branches::Vector{Schema}; const meta::NodeMeta; end

mutable struct FixedSchema <: Schema
    const name::FullName
    const aliases::Vector{String}      # normalised fullnames
    const rawaliases::Vector{String}   # as written, for re-emission
    const size::Int
    const logical::Union{Nothing,LogicalType}
    const props::Props
    const meta::NodeMeta
end

mutable struct EnumSchema <: Schema
    const name::FullName
    const aliases::Vector{String}
    const rawaliases::Vector{String}
    const doc::Union{Nothing,String}
    const symbols::Vector{String}
    const default::Default
    const symbolindex::Dict{String,Int}
    const props::Props
    const meta::NodeMeta
end

struct Field
    name::String
    schema::Schema
    doc::Union{Nothing,String}
    default::Default
    order::Symbol
    aliases::Vector{String}
    props::Props
end

mutable struct RecordSchema <: Schema
    const name::FullName
    const aliases::Vector{String}
    const rawaliases::Vector{String}
    const doc::Union{Nothing,String}
    const iserror::Bool
    const props::Props
    const fields::Vector{Field}              # filled after registration, so self-references resolve
    const fieldnames::Vector{Symbol}
    const fieldindex::Dict{Symbol,Int}
    const meta::NodeMeta
end

function RecordSchema(name::FullName, aliases::Vector{String}, rawaliases::Vector{String}, doc, iserror::Bool,
                      props::Props, meta::NodeMeta)
    return RecordSchema(name, aliases, rawaliases, doc, iserror, props, Field[], Symbol[], Dict{Symbol,Int}(), meta)
end

const NamedSchema = Union{RecordSchema,EnumSchema,FixedSchema}
const PrimitiveSchema = Union{NullSchema,BooleanSchema,IntSchema,LongSchema,FloatSchema,DoubleSchema,BytesSchema,StringSchema}

# A short name for messages and display: `record a.B`, `time-millis`, `long`.
function schemalabel(@nospecialize(s::Schema))
    s isa NamedSchema && return string(kind(s), " ", fullname(s))
    l = s isa Union{IntSchema,LongSchema,BytesSchema,StringSchema} ? s.logical : nothing
    return l === nothing ? string(kind(s)) : logicalname(l)::String
end

function kind(@nospecialize(s::Schema))
    s isa NullSchema && return :null
    s isa BooleanSchema && return :boolean
    s isa IntSchema && return :int
    s isa LongSchema && return :long
    s isa FloatSchema && return :float
    s isa DoubleSchema && return :double
    s isa BytesSchema && return :bytes
    s isa StringSchema && return :string
    s isa ArraySchema && return :array
    s isa MapSchema && return :map
    s isa UnionSchema && return :union
    s isa FixedSchema && return :fixed
    s isa EnumSchema && return :enum
    return (s::RecordSchema).iserror ? :error : :record
end

function logical(@nospecialize(s::Schema))
    s isa Union{IntSchema,LongSchema,BytesSchema,StringSchema,FixedSchema} || return nothing
    return s.logical
end

"""
    Avro.fullname(schema::NamedSchema) -> String
"""
function fullname(s::NamedSchema)
    return fullname(s.name)
end

# A node's `NodeMeta`. Naming each concrete schema type makes the field load static; `s.meta` on the abstract
# `Schema` is a property lookup at run time.
function nodemeta(@nospecialize(s::Schema))
    s isa RecordSchema && return s.meta
    s isa UnionSchema && return s.meta
    s isa StringSchema && return s.meta
    s isa LongSchema && return s.meta
    s isa IntSchema && return s.meta
    s isa ArraySchema && return s.meta
    s isa MapSchema && return s.meta
    s isa NullSchema && return s.meta
    s isa DoubleSchema && return s.meta
    s isa BooleanSchema && return s.meta
    s isa BytesSchema && return s.meta
    s isa FloatSchema && return s.meta
    s isa EnumSchema && return s.meta
    return (s::FixedSchema).meta
end

function nodeid(@nospecialize(s::Schema))
    return nodemeta(s).id
end

function graphinfo(@nospecialize(s::Schema))
    return nodemeta(s).graph::GraphInfo
end

function isfinalized(@nospecialize(s::Schema))
    return nodemeta(s).graph !== nothing
end

# Equal schemas print the same canonical form; a hash needs no more than that.
function Base.hash(@nospecialize(s::Schema), h::UInt)
    return hash(pcfbytes(s), h)
end

"""
    Avro.juliatype(schema) -> Type

The generic Julia representation of values of `schema`.
"""
function juliatype end

"""
    Avro.Schema constructors: Avro.NullSchema(; props), …, Avro.RecordSchema(name; …), Avro.Field(name, schema; …)

Public constructors validate every schema rule, reject `props` keys that collide with the structural keys
the same constructor emits, and deep-copy already-finalised children into the new graph.
"""
function NullSchema(; props=(;))
    return build(NullSchema, props)
end

function BooleanSchema(; props=(;))
    return build(BooleanSchema, props)
end

function FloatSchema(; props=(;))
    return build(FloatSchema, props)
end

function DoubleSchema(; props=(;))
    return build(DoubleSchema, props)
end

function IntSchema(; logical=nothing, props=(;))
    return build(IntSchema, props; logical=logical)
end

function LongSchema(; logical=nothing, props=(;))
    return build(LongSchema, props; logical=logical)
end

function BytesSchema(; logical=nothing, props=(;))
    return build(BytesSchema, props; logical=logical)
end

function StringSchema(; logical=nothing, props=(;))
    return build(StringSchema, props; logical=logical)
end

function Base.:(==)(a::DecimalLogical, b::DecimalLogical)
    return a.precision == b.precision && a.scale == b.scale
end

function Base.:(==)(a::UnknownLogical, b::UnknownLogical)
    return a.name == b.name
end

# ---- parse context -----------------------------------------------------------------------------

mutable struct ParseContext
    const limits::Limits
    const allow_invalid_names::Bool
    const allow_invalid_defaults::Bool
    const named::Dict{String,Schema}          # fullname → schema
    const identities::Dict{String,Schema}     # every fullname and alias, for global uniqueness
    const pending::Vector{RecordSchema}       # records registered but not yet filled
    const metas::Vector{NodeMeta}             # in creation order → dense ids
    const legacyfixednames::Bool              # legacy=:avrojl1: Avro.jl ≤ 1.1.2 wrote fixed schemas without names
    legacyfixedcount::Int                     # nameless-fixed ordinal within this document
    namedcount::Int
    namebytes::Int                            # the size of every namespace-qualified name built so far
    repaired_names::Bool
    repaired_defaults::Bool
    const path::Vector{Union{String,Int}}     # the JSON path: object keys and zero-based indices
end

function ParseContext(limits::Limits, allow_invalid_names::Bool, allow_invalid_defaults::Bool, legacyfixednames::Bool=false)
    return ParseContext(limits, allow_invalid_names, allow_invalid_defaults, Dict{String,Schema}(), Dict{String,Schema}(),
                        RecordSchema[], NodeMeta[], legacyfixednames, 0, 0, 0, false, false, Union{String,Int}[])
end

function newmeta!(ctx::ParseContext)
    m = NodeMeta()
    push!(ctx.metas, m)
    return m
end

# A JSON path (`$.fields[2].type`): each segment a member name or an index.
function formatpath(path)
    io = IOBuffer()
    print(io, '$')
    for segment in path
        segment isa String ? print(io, '.', segment) : print(io, '[', segment, ']')
    end
    return String(take!(io))
end

# A parse error at the current JSON path extended by `segments`. A parse that throws drops its context, so
# the parsers push path segments without restoring them on the way out of an error.
function schemaerror(ctx::ParseContext, msg::AbstractString, segments::Vararg{Union{String,Int},N}) where {N}
    for segment in segments
        push!(ctx.path, segment)
    end
    throw(SchemaError(String(msg), formatpath(ctx.path)))
end

const SCHEMA_GRAMMAR = Dict{Symbol,Vector{String}}(
    :null => ["type"], :boolean => ["type"], :int => ["type"], :long => ["type"], :float => ["type"],
    :double => ["type"], :bytes => ["type"], :string => ["type"],
    :array => ["type", "items"], :map => ["type", "values"],
    :record => ["type", "name", "namespace", "aliases", "doc", "fields"],
    :error => ["type", "name", "namespace", "aliases", "doc", "fields"],
    :enum => ["type", "name", "namespace", "aliases", "doc", "symbols", "default"],
    :fixed => ["type", "name", "namespace", "aliases", "size"],
)
const FIELD_GRAMMAR = ("name", "type", "doc", "default", "order", "aliases")

"The members of `obj` outside `grammar`, in source order (keys are distinct: the JSON reader rejects duplicates)."
function collectprops(obj::Props, grammar)
    members = Pair{String,Any}[]
    for (k, v) in obj
        k in grammar || push!(members, member(k, v))
    end
    return isempty(members) ? Props() : Props(members)
end

function stringattr(ctx::ParseContext, obj::Props, key::String; required::Bool=false)
    haskey(obj, key) || (required ? schemaerror(ctx, "missing required attribute \"$key\"") : return nothing)
    v = obj[key]
    v isa String || schemaerror(ctx, "attribute \"$key\" must be a JSON string", key)
    return v
end

function stringarrayattr(ctx::ParseContext, obj::Props, key::String; required::Bool=false)
    haskey(obj, key) || (required ? schemaerror(ctx, "missing required attribute \"$key\"") : return nothing)
    v = obj[key]
    v isa Vector{Any} || schemaerror(ctx, "attribute \"$key\" must be a JSON array of strings", key)
    out = String[]
    for (i, x) in enumerate(v)
        x isa String || schemaerror(ctx, "attribute \"$key\" must be a JSON array of strings", key, i - 1)
        push!(out, x)
    end
    return out
end

function checkname(ctx::ParseContext, s::AbstractString, what::AbstractString, segments::Vararg{Union{String,Int},N}) where {N}
    isvalidname(s) && return nothing
    ctx.allow_invalid_names || schemaerror(ctx, "invalid $what $(quoted(s)) (must match [A-Za-z_][A-Za-z0-9_]*)", segments...)
    ctx.repaired_names = true
    return nothing
end

function checknamespace(ctx::ParseContext, s::AbstractString, segments::Vararg{Union{String,Int},N}) where {N}
    isvalidnamespace(s) && return nothing
    ctx.allow_invalid_names || schemaerror(ctx, "invalid namespace $(quoted(s))", segments...)
    ctx.repaired_names = true
    return nothing
end

# ---- parsing ------------------------------------------------------------------------------------

"""
    Avro.parseschema(src; references=(), allow_invalid_names=false, allow_invalid_defaults=false, limits=Limits()) -> Schema

Parse an Avro schema from JSON text (a `String`, a byte vector, or an `IO` read incrementally under
`limits.max_bytes`). Every schema rule is validated; violations raise `SchemaError` (with a JSON path),
and a document over `max_bytes` or nested deeper than `max_depth` raises `LimitError`. Named references
accept strings or `{"type":"Name"}` objects; attributes on reference objects are ignored, as in Apache
Java. With `allow_invalid_names=true` invalid names are admitted (the graph is marked `repaired_names`);
with `allow_invalid_defaults=true` invalid defaults are kept with `valid=false`.

`references` lists schemas `src` may name without defining them, for types shared across files: each is
JSON text or a `Schema`, may name those listed before it, and defines each of its named types only once
across the list. The result holds only the types reachable from `src`.
"""
function parseschema(src; references=(), allow_invalid_names::Bool=false, allow_invalid_defaults::Bool=false, limits::Limits=Limits())
    return parseschemaimpl(src, false; references=references, allow_invalid_names=allow_invalid_names,
        allow_invalid_defaults=allow_invalid_defaults, limits=limits)
end

function parseschemaimpl(src, legacyfixednames::Bool; references=(), allow_invalid_names::Bool=false,
                         allow_invalid_defaults::Bool=false, limits::Limits=Limits())
    errfn = (msg, pos) -> throw(SchemaError(string(msg, " (byte ", pos, ")"), "\$"))
    tree(x) = parsejson(sourcebytes(x isa Schema ? json(x) : x, limits.max_bytes); limits=limits, errfn=errfn)
    ctx = ParseContext(limits, allow_invalid_names, allow_invalid_defaults, legacyfixednames)
    for reference in references
        parsenode(ctx, tree(reference), "")
    end
    s = parsenode(ctx, tree(src), "")
    isempty(ctx.pending) || schemaerror(ctx, "internal error: unfilled record")
    return finalize!(ctx, s)
end

"""
    sourcebytes(src, maxbytes) -> Vector{UInt8}

Normalise a source to a byte vector: a `String` or `Vector{UInt8}` is used in place (no copy), other
strings and byte vectors are copied, and an `IO` is read incrementally, raising `LimitError(:max_bytes, …)`
once more than `maxbytes` bytes arrive.
"""
function sourcebytes(src::String, maxbytes::Int)
    return unsafe_wrap(Vector{UInt8}, src)   # shares the string's bytes and keeps it alive; never mutated
end

function sourcebytes(src::AbstractString, maxbytes::Int)
    return Vector{UInt8}(codeunits(src))
end

function sourcebytes(src::Vector{UInt8}, maxbytes::Int)
    return src
end

function sourcebytes(src::AbstractVector{UInt8}, maxbytes::Int)
    return copyto!(Vector{UInt8}(undef, length(src)), src)
end

function sourcebytes(io::IO, maxbytes::Int)
    out = UInt8[]
    chunk = Vector{UInt8}(undef, 64 * KiB)
    while !eof(io)
        n = readbytes!(io, chunk)
        n == 0 && break
        total = length(out) + n
        total <= maxbytes || throw(LimitError(:max_bytes, total, maxbytes))
        append!(out, view(chunk, 1:n))
    end
    return out
end

function parsenode(ctx::ParseContext, @nospecialize(node), enclosing::String)
    node isa String && return parsereference(ctx, node, enclosing)
    node isa Vector{Any} && return parseunion(ctx, node, enclosing)
    node isa Props && return parseobjectschema(ctx, node, enclosing)
    schemaerror(ctx, "a schema must be a JSON string, object or array")
end

function parsenode(ctx::ParseContext, @nospecialize(node), enclosing::String, key::String)
    push!(ctx.path, key)
    s = parsenode(ctx, node, enclosing)
    pop!(ctx.path)
    return s
end

function parsereference(ctx::ParseContext, name::String, enclosing::String)
    name in PRIMITIVE_NAMES && return primitive(ctx, name, Props())
    dotted = lastdot(name) !== nothing
    key = dotted || isempty(enclosing) ? name : string(enclosing, ".", name)
    s = get(ctx.named, key, nothing)
    s === nothing && !isempty(enclosing) &&
        (s = get(ctx.named, name, nothing))   # a bare reference may name a null-namespace type
    s === nothing && schemaerror(ctx, "undefined type $(quoted(name)) (types must be defined before use)")
    return s
end

function primitive(ctx::ParseContext, name::String, p::Props)
    meta = newmeta!(ctx)
    name == "null" && return NullSchema(p, meta)
    name == "boolean" && return BooleanSchema(p, meta)
    name == "int" && return IntSchema(evaluatelogical(:int, 0, p), p, meta)
    name == "long" && return LongSchema(evaluatelogical(:long, 0, p), p, meta)
    name == "float" && return FloatSchema(p, meta)
    name == "double" && return DoubleSchema(p, meta)
    name == "bytes" && return BytesSchema(evaluatelogical(:bytes, 0, p), p, meta)
    return StringSchema(evaluatelogical(:string, 0, p), p, meta)
end

"A union branch's identity: named branches by fullname, every other branch by its kind."
function branchidentity(@nospecialize(s::Schema))
    return (s isa NamedSchema, branchidentitylabel(s))
end

function branchidentitylabel(@nospecialize(s::Schema))
    s isa NamedSchema && return fullname(s)
    return string(kind(s))
end

function parseunion(ctx::ParseContext, arr::Vector{Any}, enclosing::String)
    branches = Schema[]
    seen = Set{Tuple{Bool,String}}()
    for (i, b) in enumerate(arr)
        push!(ctx.path, i - 1)
        b isa Vector{Any} && schemaerror(ctx, "unions may not immediately contain other unions")
        s = parsenode(ctx, b, enclosing)
        s isa UnionSchema && schemaerror(ctx, "unions may not immediately contain other unions")
        identity = branchidentity(s)
        identity in seen && schemaerror(ctx, "duplicate union branch $(branchidentitylabel(s))")
        push!(seen, identity)
        push!(branches, s)
        pop!(ctx.path)
    end
    return UnionSchema(branches, newmeta!(ctx))
end

function parseobjectschema(ctx::ParseContext, obj::Props, enclosing::String)
    haskey(obj, "type") || schemaerror(ctx, "schema object without a \"type\" attribute")
    t = obj["type"]
    t isa String || schemaerror(ctx, "a schema object's \"type\" must be a JSON string naming a primitive, a named type, or one of record, error, enum, array, map, fixed", "type")
    if t in PRIMITIVE_NAMES
        return primitive(ctx, t, collectprops(obj, ("type",)))
    elseif t == "array"
        haskey(obj, "items") || schemaerror(ctx, "array schema without \"items\"")
        items = parsenode(ctx, obj["items"], enclosing, "items")
        return ArraySchema(items, collectprops(obj, SCHEMA_GRAMMAR[:array]), newmeta!(ctx))
    elseif t == "map"
        haskey(obj, "values") || schemaerror(ctx, "map schema without \"values\"")
        values = parsenode(ctx, obj["values"], enclosing, "values")
        return MapSchema(values, collectprops(obj, SCHEMA_GRAMMAR[:map]), newmeta!(ctx))
    elseif t == "record" || t == "error"
        return parserecord(ctx, obj, enclosing, t == "error")
    elseif t == "enum"
        return parseenum(ctx, obj, enclosing)
    elseif t == "fixed"
        return parsefixed(ctx, obj, enclosing)
    end
    return parsereference(ctx, t, enclosing)
end

"The fullname algorithm: a dotted name splits at its last dot; otherwise the namespace attribute, then the enclosing one."
function resolvefullname(name::AbstractString, namespace::Union{Nothing,AbstractString}, enclosing::AbstractString)
    dot = lastdot(name)
    dot === nothing && return FullName(String(name), String(namespace === nothing ? enclosing : namespace))
    return FullName(String(SubString(name, nextind(name, dot))), String(SubString(name, firstindex(name), prevind(name, dot))))
end

"An alias normalised against the type's namespace (a dotted alias is already a fullname)."
function normalizedalias(alias::String, namespace::String)
    (lastdot(alias) !== nothing || isempty(namespace)) && return alias
    return string(namespace, ".", alias)
end

# `findlast('.', s)` without Base's unspecialised predicate, which `--trim` cannot resolve. A dot is one code
# unit in every string encoding.
function lastdot(s::AbstractString)
    for i in ncodeunits(s):-1:1
        codeunit(s, i) == UInt8('.') && return i
    end
    return nothing
end

"Normalise `raw` aliases: drop the type's own name and duplicates, keep declaration order."
function normalizedaliases(raw::Vector{String}, full::FullName)
    canonical = fullname(full)
    aliases = String[]
    seen = Set{String}()
    for a in raw
        na = normalizedalias(a, full.namespace)
        (na == canonical || na in seen) && continue
        push!(seen, na)
        push!(aliases, na)
    end
    return aliases
end

# A long namespace qualifies every name in it, so the names built can far exceed the text that spelled them.
function chargenames!(ctx::ParseContext, bytes::Int)
    ctx.namebytes = satadd(ctx.namebytes, bytes)
    ctx.namebytes <= ctx.limits.max_bytes || throw(LimitError(:max_bytes, ctx.namebytes, ctx.limits.max_bytes))
    return nothing
end

function parsenamed(ctx::ParseContext, obj::Props, enclosing::String)
    name = stringattr(ctx, obj, "name"; required=true)
    namespace = get(obj, "namespace", nothing)
    namespace === nothing || namespace isa String || schemaerror(ctx, "attribute \"namespace\" must be a JSON string", "namespace")
    full = resolvefullname(name, namespace, enclosing)
    checkname(ctx, full.name, "name", "name")
    checknamespace(ctx, full.namespace, "namespace")
    isreservedfullname(full) && schemaerror(ctx, "\"$(full.name)\" is a primitive type name and cannot be redefined in the null namespace", "name")
    chargenames!(ctx, sizeof(full.name) + sizeof(full.namespace) + 1)
    haskey(ctx.named, fullname(full)) && schemaerror(ctx, "duplicate type name", "name")
    ctx.namedcount += 1
    raw = stringarrayattr(ctx, obj, "aliases")
    raw === nothing && return full, String[], String[]
    chargenames!(ctx, sum(a -> sizeof(a) + sizeof(full.namespace) + 1, raw; init=0))
    return full, normalizedaliases(raw, full), raw
end

function register!(ctx::ParseContext, s::NamedSchema)
    full = fullname(s)
    haskey(ctx.identities, full) && schemaerror(ctx, "a name or alias is used by more than one type")
    for alias in s.aliases
        haskey(ctx.identities, alias) && schemaerror(ctx, "name or alias \"$alias\" is used by more than one type")
    end
    ctx.named[full] = s
    ctx.identities[full] = s
    for alias in s.aliases
        ctx.identities[alias] = s
    end
    return s
end

function parsefixed(ctx::ParseContext, obj::Props, enclosing::String)
    full, aliases, raw = if ctx.legacyfixednames && !haskey(obj, "name")
        # Avro.jl ≤ 1.1.2 wrote nameless fixed schemas (they carry no references, so a synthetic name is unambiguous)
        ctx.legacyfixedcount += 1
        (FullName(string("_avrojl1_fixed_", ctx.legacyfixedcount), ""), String[], String[])
    else
        parsenamed(ctx, obj, enclosing)
    end
    haskey(obj, "size") || schemaerror(ctx, "fixed schema without \"size\"")
    sz = obj["size"]
    sz isa Int64 && sz >= 0 || schemaerror(ctx, "fixed \"size\" must be a non-negative JSON integer", "size")
    size = Int(sz::Int64)
    p = collectprops(obj, SCHEMA_GRAMMAR[:fixed])
    return register!(ctx, FixedSchema(full, aliases, raw, size, evaluatelogical(:fixed, size, p), p, newmeta!(ctx)))
end

function parseenum(ctx::ParseContext, obj::Props, enclosing::String)
    full, aliases, raw = parsenamed(ctx, obj, enclosing)
    syms = stringarrayattr(ctx, obj, "symbols"; required=true)
    index = Dict{String,Int}()
    for (i, sym) in enumerate(syms)
        checkname(ctx, sym, "enum symbol", "symbols", i - 1)
        haskey(index, sym) && schemaerror(ctx, "duplicate enum symbol $(quoted(sym))", "symbols", i - 1)
        index[sym] = i
    end
    doc = stringattr(ctx, obj, "doc")
    default = nodefault
    if haskey(obj, "default")
        d = obj["default"]
        if d isa String && haskey(index, d)
            default = DefaultValue(d, 0, index[d], true)
        else
            ctx.allow_invalid_defaults || schemaerror(ctx, "enum default must be one of the symbols", "default")
            ctx.repaired_defaults = true
            default = DefaultValue(d, 0, 0, false)
        end
    end
    p = collectprops(obj, SCHEMA_GRAMMAR[:enum])
    return register!(ctx, EnumSchema(full, aliases, raw, doc, syms, default, index, p, newmeta!(ctx)))
end

function parserecord(ctx::ParseContext, obj::Props, enclosing::String, iserror::Bool)
    full, aliases, raw = parsenamed(ctx, obj, enclosing)
    haskey(obj, "fields") || schemaerror(ctx, "record schema without \"fields\"")
    farr = obj["fields"]
    farr isa Vector{Any} || schemaerror(ctx, "\"fields\" must be a JSON array", "fields")
    doc = stringattr(ctx, obj, "doc")
    rec = RecordSchema(full, aliases, raw, doc, iserror, collectprops(obj, SCHEMA_GRAMMAR[:record]), newmeta!(ctx))
    register!(ctx, rec)                            # register before filling so self-references resolve
    push!(ctx.pending, rec)
    for (i, f) in enumerate(farr)
        push!(ctx.path, "fields")
        push!(ctx.path, i - 1)
        f isa Props || schemaerror(ctx, "each field must be a JSON object")
        push!(rec.fields, parsefield(ctx, f, full.namespace))
        pop!(ctx.path)
        pop!(ctx.path)
    end
    indexfields!(rec, (i, msg, key) -> schemaerror(ctx, msg, "fields", i - 1, key))
    pop!(ctx.pending)
    return rec
end

"""
    indexfields!(rec, errfn)

Fill `fieldnames`/`fieldindex` from `rec.fields`, rejecting duplicate names and alias collisions through
`errfn(fieldposition, message, attribute)`.
"""
function indexfields!(rec::RecordSchema, errfn)
    owners = Dict{String,Int}()
    for (i, f) in enumerate(rec.fields)
        haskey(rec.fieldindex, fieldsymbol(f.name)) && errfn(i, "duplicate field name $(quoted(f.name))", "name")
        push!(rec.fieldnames, fieldsymbol(f.name))
        rec.fieldindex[fieldsymbol(f.name)] = i
    end
    for (i, f) in enumerate(rec.fields), alias in f.aliases
        haskey(rec.fieldindex, fieldsymbol(alias)) && errfn(i, "field alias $(quoted(alias)) collides with a field name", "aliases")
        haskey(owners, alias) && errfn(i, "field alias $(quoted(alias)) is declared by two fields", "aliases")
        owners[alias] = i
    end
    return rec
end

# A NUL byte cannot be part of a `Symbol`; such names only arrive under `allow_invalid_names`.
function fieldsymbol(name::String)
    return Symbol(occursin('\0', name) ? replace(name, '\0' => "\\u0000") : name)
end

function parsefield(ctx::ParseContext, obj::Props, ns::String)
    name = stringattr(ctx, obj, "name"; required=true)
    checkname(ctx, name, "field name", "name")
    haskey(obj, "type") || schemaerror(ctx, "field without \"type\"")
    schema = parsenode(ctx, obj["type"], ns, "type")
    doc = stringattr(ctx, obj, "doc")
    order = :ascending
    if haskey(obj, "order")
        o = obj["order"]
        o isa String && o in ("ascending", "descending", "ignore") || schemaerror(ctx, "\"order\" must be \"ascending\", \"descending\" or \"ignore\"", "order")
        order = Symbol(o::String)
    end
    raw = stringarrayattr(ctx, obj, "aliases")
    aliases = raw === nothing ? String[] : fieldaliases(raw, name)
    default = nodefault
    haskey(obj, "default") && (default = DefaultValue(obj["default"], 0, 0, true))    # checked once the graph is complete
    return Field(name, schema, doc, default, order, aliases, collectprops(obj, FIELD_GRAMMAR))
end

"Field aliases without the field's own name or duplicates, in declaration order."
function fieldaliases(raw, name::AbstractString)
    return unique!(String[String(a) for a in raw if a != name])
end

# ---- the recursive default rule ---------------------------------------------------------

# Each (schema, JSON container) pair checked so far and its (ok, branch) answer.
const DefaultMemo = Dict{Tuple{UInt,UInt},Tuple{Bool,Int}}

"""
    validatedefault(schema, json, memo=DefaultMemo()) -> (ok::Bool, branch::Int)

Validate a JSON default against a schema: union defaults are the bare JSON of the value matched
against the branches in declaration order (returning the 1-based branch); record defaults may omit
fields that have their own defaults and ignore unknown members; every other kind follows the strict
JSON encoding of a datum. `memo` keeps the work linear however many paths reach a pair. A pair reached
again while it is being checked is a recursive default with no finite expansion, so it does not match.
"""
function validatedefault(@nospecialize(schema::Schema), @nospecialize(json), memo::DefaultMemo=DefaultMemo())
    json isa Union{Props,Vector{Any}} || return checkdefault(schema, json, memo)
    key = (objectid(schema), objectid(json))
    known = get(memo, key, nothing)
    known === nothing || return known
    memo[key] = (false, 0)
    result = checkdefault(schema, json, memo)
    memo[key] = result
    return result
end

function checkdefault(@nospecialize(schema::Schema), @nospecialize(json), memo::DefaultMemo)
    if schema isa UnionSchema
        for (i, b) in enumerate(schema.branches)
            first(validatedefault(b, json, memo)) && return (true, i)
        end
        return (false, 0)
    elseif schema isa ArraySchema
        json isa Vector{Any} || return (false, 0)
        for x in json
            first(validatedefault(schema.items, x, memo)) || return (false, 0)
        end
        return (true, 0)
    elseif schema isa MapSchema
        json isa Props || return (false, 0)
        for (k, v) in json
            isstrictutf8(k) && first(validatedefault(schema.values, v, memo)) || return (false, 0)
        end
        return (true, 0)
    elseif schema isa RecordSchema
        json isa Props || return (false, 0)
        members = memberlookup(json)
        for f in schema.fields
            child = get(members, f.name, nodefault)
            if child === nodefault                     # unknown members are ignored; omitted fields take their own default
                f.default isa DefaultValue || return (false, 0)
                child = f.default.json
            end
            first(validatedefault(f.schema, child, memo)) || return (false, 0)
        end
        return (true, 0)
    end
    return (defaultvalue(schema, json) !== nothing, 0)       # a leaf default is valid when it converts
end

# JSON objects scan their members, so looking up many fields goes through an index.
function memberlookup(json::Props)
    return length(json) <= 8 ? json : Dict{String,Any}(json)
end

function replacedefault!(record::RecordSchema, index::Int, branch::Int, valid::Bool)
    field = record.fields[index]
    old = field.default::DefaultValue
    old.branch == branch && old.valid == valid && return nothing
    record.fields[index] = Field(field.name, field.schema, field.doc, DefaultValue(old.json, branch, old.index, valid),
                                 field.order, field.aliases, field.props)
    return nothing
end

function invalidgraphdefault!(ctx::ParseContext, record::RecordSchema, field::Field)
    ctx.allow_invalid_defaults ||
        schemaerror(ctx, "default value for field $(quoted(field.name)) of record $(fullname(record)) does not match its schema")
    ctx.repaired_defaults = true
    return nothing
end

function invalidgraphdefault!(::Nothing, record::RecordSchema, field::Field)
    throw(ArgumentError("default value for field \"$(field.name)\" of record $(fullname(record)) does not match its schema"))
end

# Once every record is filled, check each field default and record its union branch.
function validategraphdefaults!(@nospecialize(root::Schema), nodes::Int, ctx::Union{Nothing,ParseContext})
    validategraphdefaultnode!(root, falses(nodes), DefaultMemo(), ctx)
    return root
end

function validategraphdefaultnode!(@nospecialize(schema::Schema), seen::BitVector, memo::DefaultMemo, ctx::Union{Nothing,ParseContext})
    index = Int(nodeid(schema)) + 1
    seen[index] && return nothing
    seen[index] = true
    if schema isa ArraySchema
        validategraphdefaultnode!(schema.items, seen, memo, ctx)
    elseif schema isa MapSchema
        validategraphdefaultnode!(schema.values, seen, memo, ctx)
    elseif schema isa UnionSchema
        for branch in schema.branches
            validategraphdefaultnode!(branch, seen, memo, ctx)
        end
    elseif schema isa RecordSchema
        for (i, field) in enumerate(schema.fields)
            if field.default isa DefaultValue
                valid, branch = validatedefault(field.schema, field.default.json, memo)
                valid || invalidgraphdefault!(ctx, schema, field)
                replacedefault!(schema, i, valid ? branch : 0, valid)
            end
            validategraphdefaultnode!(field.schema, seen, memo, ctx)
        end
    end
    return nothing
end

# How deep a parsed schema nests as JSON, following references (the edge closing a cycle adds nothing);
# props and defaults are not counted. Recursive walks of a parsed graph stay within this depth.
function nesteddepth(@nospecialize(s::Schema), known::Vector{Int}, depth::Int, limit::Int)
    i = Int(nodeid(s)) + 1
    known[i] >= 0 && return known[i]
    known[i] = 0
    levels = s isa RecordSchema ? 3 : s isa Union{ArraySchema,MapSchema,UnionSchema} ? 1 : 0
    depth + levels > limit && throw(LimitError(:max_depth, depth + levels, limit))
    below = 0
    if s isa RecordSchema
        for f in s.fields
            below = max(below, nesteddepth(f.schema, known, depth + levels, limit))
        end
    elseif s isa UnionSchema
        for b in s.branches
            below = max(below, nesteddepth(b, known, depth + levels, limit))
        end
    elseif s isa ArraySchema
        below = nesteddepth(s.items, known, depth + levels, limit)
    elseif s isa MapSchema
        below = nesteddepth(s.values, known, depth + levels, limit)
    end
    levels + below > limit && throw(LimitError(:max_depth, levels + below, limit))
    known[i] = levels + below
    return known[i]
end

# ---- finalisation: ids, graph info, least sizes ---------------------------------------------------

function finalize!(ctx::ParseContext, @nospecialize(root::Schema))
    for (i, m) in enumerate(ctx.metas)
        m.id = Int32(i - 1)
    end
    nesteddepth(root, fill(-1, length(ctx.metas)), 0, ctx.limits.max_depth)
    validategraphdefaults!(root, length(ctx.metas), ctx)
    return sealgraph!(root, ctx.metas, GraphInfo(ctx.repaired_names, ctx.repaired_defaults, length(ctx.metas), ctx.namedcount))
end

# Give every node of the graph at `root` its shared `info`, then its least encoded size.
function sealgraph!(@nospecialize(root::Schema), metas::Vector{NodeMeta}, info::GraphInfo)
    for m in metas
        m.graph = info
    end
    computeminsizes!(root, length(metas))
    return root
end

# ---- structural equality --------------------------------------------------------------------------

"""
    ==(a::Schema, b::Schema)

Structural equality: kind, fullname, fields (name, schema, default value and selected branch, order,
normalised aliases, doc), symbols, enum default, size, logical type, `iserror`, and `props` as unordered
maps of JSON values (number tokens compare lexically). Cyclic graphs terminate through a visited-pair set.
"""
function Base.:(==)(@nospecialize(a::Schema), @nospecialize(b::Schema))
    return schemaequal(a, b)
end

function schemaequal(@nospecialize(a::Schema), @nospecialize(b::Schema), visited::Set{Tuple{UInt,UInt}}=Set{Tuple{UInt,UInt}}())
    a === b && return true
    typeof(a) === typeof(b) || return false
    pair = (objectid(a), objectid(b))
    pair in visited && return true
    push!(visited, pair)
    return structuralequal(a, b, visited)
end

# `a` and `b` have the same concrete type.
function structuralequal(@nospecialize(a::Schema), @nospecialize(b::Schema), visited::Set{Tuple{UInt,UInt}})
    if a isa ArraySchema
        y = b::ArraySchema
        return jsonequal(a.props, y.props) && schemaequal(a.items, y.items, visited)
    elseif a isa MapSchema
        y = b::MapSchema
        return jsonequal(a.props, y.props) && schemaequal(a.values, y.values, visited)
    elseif a isa UnionSchema
        y = b::UnionSchema
        length(a.branches) == length(y.branches) || return false
        for i in eachindex(a.branches, y.branches)
            schemaequal(a.branches[i], y.branches[i], visited) || return false
        end
        return true
    elseif a isa FixedSchema
        y = b::FixedSchema
        return a.name == y.name && a.size == y.size && a.aliases == y.aliases && logicalequal(a.logical, y.logical) &&
            jsonequal(a.props, y.props)
    elseif a isa EnumSchema
        y = b::EnumSchema
        return a.name == y.name && a.symbols == y.symbols && a.aliases == y.aliases && a.doc == y.doc &&
            defaultequal(a.default, y.default) && jsonequal(a.props, y.props)
    elseif a isa RecordSchema
        y = b::RecordSchema
        a.name == y.name && a.iserror == y.iserror && a.aliases == y.aliases && a.doc == y.doc &&
            jsonequal(a.props, y.props) || return false
        length(a.fields) == length(y.fields) || return false
        for (f, g) in zip(a.fields, y.fields)
            f.name == g.name && f.order == g.order && f.aliases == g.aliases && f.doc == g.doc &&
                jsonequal(f.props, g.props) || return false
            defaultequal(f.default, g.default) || return false
            schemaequal(f.schema, g.schema, visited) || return false
        end
        return true
    end
    return logicalequal(logical(a), logical(b)) && jsonequal((a::PrimitiveSchema).props, (b::PrimitiveSchema).props)
end

function logicalequal(@nospecialize(a::Union{Nothing,LogicalType}), @nospecialize(b::Union{Nothing,LogicalType}))
    a isa DecimalLogical && return b isa DecimalLogical && a == b
    a isa UnknownLogical && return b isa UnknownLogical && a == b
    return a === b
end

function defaultequal(a::Default, b::Default)
    a isa NoDefault && return b isa NoDefault
    b isa DefaultValue || return false
    return a.valid == b.valid && a.branch == b.branch && jsonequal(a.json, b.json)
end

# `==` on parsed JSON values (`nothing`, `Bool`, `Int64`, `JSONNumber`, `String`, `Vector{Any}`, and `Props`
# as unordered maps), comparing nested values without dispatching on their types at run time.
function jsonequal(@nospecialize(a), @nospecialize(b))
    if a isa Props
        b isa Props || return false
        length(a) == length(b) || return false
        members = memberlookup(b)
        for (k, v) in a
            w = get(members, k, nodefault)
            w === nodefault && return false
            jsonequal(v, w) || return false
        end
        return true
    elseif a isa Vector{Any}
        b isa Vector{Any} || return false
        length(a) == length(b) || return false
        for i in eachindex(a, b)
            jsonequal(a[i], b[i]) || return false
        end
        return true
    elseif a isa String
        return b isa String && a == b
    elseif a isa JSONNumber
        return b isa JSONNumber && a == b
    elseif a isa Union{Bool,Int64}
        return b isa Union{Bool,Int64} && a == b
    end
    return a === b
end

# ---- printing ---------------------------------------------------------------------------------------

"""
    Avro.json(schema; pretty=false) -> String

The schema as spec JSON: the first occurrence of a named type in full, later references by name (short
inside the same namespace),
the namespace attribute only when it differs from the enclosing one, custom `props` and defaults
re-emitted from their parsed JSON values (number tokens verbatim, strings re-escaped).
"""
function json(@nospecialize(s::Schema); pretty::Bool=false)
    io = IOBuffer()
    printjson(io, schematree(s, "", schemaseen(s)), pretty, 0)
    return String(take!(io))
end

"A fresh node-indexed visited table for the schema printers."
function schemaseen(@nospecialize(s::Schema))
    return fill(false, graphinfo(s).nodes)
end

function indent(io::IO, pretty::Bool, level::Int)
    pretty || return nothing
    print(io, '\n')
    for _ in 1:level
        print(io, "  ")
    end
    return nothing
end

# A JSON value printed on one line even in pretty output (defaults, aliases, symbols).
struct OneLine
    value::Any
end

# A member of a `Vector{Pair{String,Any}}` JSON object. `k => v` would build a `Pair` of `v`'s type and
# convert it again on the way into the vector, through a call `--trim` cannot resolve.
function member(k::String, @nospecialize(v))
    return Pair{String,Any}(k, v)
end

function printjson(io::IO, @nospecialize(x), pretty::Bool, level::Int)
    if x === nothing
        print(io, "null")
    elseif x isa Bool
        print(io, x ? "true" : "false")
    elseif x isa Int64
        print(io, x)
    elseif x isa JSONNumber
        print(io, x.text)
    elseif x isa String
        escapejson(io, x)
    elseif x isa OneLine
        printjson(io, x.value, false, 0)
    elseif x isa Props
        printobject(io, x, pretty, level)
    elseif x isa Vector{Pair{String,Any}}
        printobject(io, x, pretty, level)
    elseif x isa Vector{Any}
        printarray(io, x, pretty, level)
    elseif x isa Vector{String}
        printarray(io, x, pretty, level)
    else
        throw(ArgumentError("cannot print $(typeof(x)) as JSON"))
    end
    return nothing
end

# Called with one concrete container type at a time, since a loop over a `Union` of containers iterates a
# `Union` of iteration states. `enumerate` over a `Props` splats its `Union`-typed state, so this loop counts
# members itself.
function printobject(io::IO, x::Union{Props,Vector{Pair{String,Any}}}, pretty::Bool, level::Int)
    print(io, '{')
    i = 0
    for (k, v) in x
        (i += 1) > 1 && print(io, ',')
        indent(io, pretty, level + 1)
        escapejson(io, k)
        print(io, pretty ? ": " : ":")
        printjson(io, v, pretty, level + 1)
    end
    isempty(x) || indent(io, pretty, level)
    print(io, '}')
    return nothing
end

function printarray(io::IO, x::Union{Vector{Any},Vector{String}}, pretty::Bool, level::Int)
    print(io, '[')
    for (i, v) in enumerate(x)
        i > 1 && print(io, ',')
        indent(io, pretty, level + 1)
        printjson(io, v, pretty, level + 1)
    end
    isempty(x) || indent(io, pretty, level)
    print(io, ']')
    return nothing
end

function withprops(members::Vector{Pair{String,Any}}, props::Props)
    for (k, v) in props
        push!(members, member(k, v))
    end
    return members
end

# The schema's spec JSON as values for `printjson`: the first occurrence of a named type in full, later
# references by name, the namespace only when it differs from the enclosing one.
function schematree(@nospecialize(s::Schema), enclosing::String, seen::Vector{Bool})
    if s isa PrimitiveSchema
        isempty(s.props) && return String(kind(s))
        return withprops(Pair{String,Any}[member("type", String(kind(s)))], s.props)
    elseif s isa UnionSchema
        branches = Vector{Any}(undef, length(s.branches))
        for (i, b) in enumerate(s.branches)
            branches[i] = schematree(b, enclosing, seen)
        end
        return branches
    elseif s isa ArraySchema
        return withprops(Pair{String,Any}[member("type", "array"), member("items", schematree(s.items, enclosing, seen))], s.props)
    elseif s isa MapSchema
        return withprops(Pair{String,Any}[member("type", "map"), member("values", schematree(s.values, enclosing, seen))], s.props)
    end
    n = s::NamedSchema
    index = Int(nodeid(n)) + 1
    seen[index] && return n.name.namespace == enclosing ? n.name.name : fullname(n)
    seen[index] = true
    m = Pair{String,Any}[member("type", String(kind(n))), member("name", n.name.name)]
    n.name.namespace == enclosing || push!(m, member("namespace", n.name.namespace))
    isempty(n.rawaliases) || push!(m, member("aliases", OneLine(n.rawaliases)))
    if n isa FixedSchema
        push!(m, member("size", Int64(n.size)))
        return withprops(m, n.props)
    end
    n.doc === nothing || push!(m, member("doc", n.doc))
    if n isa EnumSchema
        push!(m, member("symbols", OneLine(n.symbols)))
        n.default isa DefaultValue && push!(m, member("default", OneLine(n.default.json)))
    else
        fields = Vector{Any}(undef, length(n.fields))
        for (i, f) in enumerate(n.fields)
            fields[i] = fieldtree(f, n.name.namespace, seen)
        end
        push!(m, member("fields", fields))
    end
    return withprops(m, n.props)
end

function fieldtree(f::Field, namespace::String, seen::Vector{Bool})
    m = Pair{String,Any}[member("name", f.name), member("type", schematree(f.schema, namespace, seen))]
    f.doc === nothing || push!(m, member("doc", f.doc))
    f.default isa DefaultValue && push!(m, member("default", OneLine(f.default.json)))
    f.order == :ascending || push!(m, member("order", String(f.order)))
    isempty(f.aliases) || push!(m, member("aliases", OneLine(f.aliases)))
    return withprops(m, f.props)
end

function Base.show(io::IO, @nospecialize(s::Schema))
    return print(io, "Avro.Schema(", json(s), ")")
end

function Base.show(io::IO, ::MIME"text/plain", @nospecialize(s::Schema))
    print(io, "Avro.Schema ", json(s; pretty=true))
    return nothing
end

# ---- public constructors ---------------------------------------------------------------------------

function build(::Type{T}, propsin; logical=nothing) where {T<:PrimitiveSchema}
    k = T === IntSchema ? :int : T === LongSchema ? :long : T === BytesSchema ? :bytes : T === StringSchema ? :string : nothing
    k === nothing && return finalizepublic!(T(makeprops(propsin, ("type",), nothing), NodeMeta()))
    p = makeprops(propsin, ("type",), logical)
    return finalizepublic!(T(evaluatelogical(k, 0, p), p, NodeMeta()))
end

"Custom attributes from a NamedTuple, dict or pair iterator, plus the attributes a `logical=` synthesises."
function makeprops(propsin, structural, logical)
    members = Pair{String,Any}[]
    seen = Set{String}()
    for (k, v) in (propsin isa NamedTuple ? pairs(propsin) : propsin)
        k isa Union{AbstractString,Symbol} || throw(ArgumentError("`props` keys must be strings or symbols"))
        ks = String(k)
        ks in structural && throw(ArgumentError("`props` key \"$ks\" collides with a structural attribute emitted by this constructor"))
        logical !== nothing && ks in ("logicalType", "precision", "scale") &&
            throw(ArgumentError("`props` key \"$ks\" is synthesised by `logical=`"))
        ks in seen && throw(ArgumentError("duplicate `props` key \"$ks\""))
        push!(seen, ks)
        push!(members, ks => tojsonvalue(v))
    end
    if logical !== nothing
        name = logicalname(logical)
        name === nothing && throw(ArgumentError("no logical type name for $logical"))
        push!(members, "logicalType" => name)
        if logical isa DecimalLogical
            push!(members, "precision" => Int64(logical.precision))
            push!(members, "scale" => Int64(logical.scale))
        end
    end
    return isempty(members) ? Props() : Props(members)
end

"A Julia value as a schema JSON value (`nothing`, `Bool`, `Int64`, `JSONNumber`, `String`, `Vector{Any}`, `Props`)."
function tojsonvalue(x)
    throw(ArgumentError("a $(typeof(x)) has no JSON representation"))
end

function tojsonvalue(x::Union{Nothing,Bool,Int64,JSONNumber,String})
    return x
end

function tojsonvalue(x::Integer)
    return Int64(x)
end

function tojsonvalue(x::AbstractFloat)
    isfinite(x) && return JSONNumber(repr(Float64(x)))
    return isnan(x) ? "NaN" : (x > 0 ? "Infinity" : "-Infinity")
end

function tojsonvalue(x::Union{AbstractString,Symbol})
    return String(x)
end

function tojsonvalue(x::AbstractVector)
    return Any[tojsonvalue(e) for e in x]
end

function tojsonvalue(x::Union{AbstractDict,NamedTuple})
    members = Pair{String,Any}[]
    seen = Set{String}()
    for (k, v) in pairs(x)
        k isa Union{AbstractString,Symbol} || throw(ArgumentError("JSON object keys must be strings or symbols"))
        ks = String(k)
        ks in seen && throw(ArgumentError("duplicate key \"$ks\""))
        push!(seen, ks)
        push!(members, ks => tojsonvalue(v))
    end
    return Props(members)
end

# Marks a record `RecordSchema(f, name)` is still filling. A graph reaching one stays open (its nodes keep
# no ids) until the outermost record closes.
const FILLING = Int32(-2)

function reachesfilling(s::Schema, seen::Base.IdSet{Schema}=Base.IdSet{Schema}())
    (isfinalized(s) || s in seen) && return false
    push!(seen, s)
    nodemeta(s).id == FILLING && return true
    s isa ArraySchema && return reachesfilling(s.items, seen)
    s isa MapSchema && return reachesfilling(s.values, seen)
    s isa UnionSchema && return any(b -> reachesfilling(b, seen), s.branches)
    s isa RecordSchema && return any(f -> reachesfilling(f.schema, seen), s.fields)
    return false
end

"Finalise a constructed graph: dense ids, named-type uniqueness, the complete-graph default pass."
function finalizepublic!(draft::Schema)
    reachesfilling(draft) && return draft
    s = deepcopyschema(draft, IdDict{Schema,Schema}())   # every graph owns its nodes; a child passed twice is one definition
    metas = NodeMeta[]
    named = Dict{String,Schema}()
    identities = Dict{String,Schema}()
    collectmetas!(s, metas, named, identities)
    validategraphdefaults!(s, length(metas), nothing)
    return sealgraph!(s, metas, GraphInfo(false, false, length(metas), length(named)))
end

function collectmetas!(s::Schema, metas::Vector{NodeMeta}, named::Dict{String,Schema}, identities::Dict{String,Schema})
    m = nodemeta(s)
    m.id >= 0 && return nothing
    m.id = Int32(length(metas))
    push!(metas, m)
    if s isa NamedSchema
        full = fullname(s)
        haskey(named, full) && throw(ArgumentError("a named schema is defined more than once"))
        haskey(identities, full) && throw(ArgumentError("a named schema name or alias is used by more than one type"))
        for alias in s.aliases
            haskey(identities, alias) && throw(ArgumentError("named schema name or alias \"$alias\" is used by more than one type"))
        end
        named[full] = s
        identities[full] = s
        for alias in s.aliases
            identities[alias] = s
        end
    end
    s isa ArraySchema && collectmetas!(s.items, metas, named, identities)
    s isa MapSchema && collectmetas!(s.values, metas, named, identities)
    s isa UnionSchema && foreach(b -> collectmetas!(b, metas, named, identities), s.branches)
    s isa RecordSchema && foreach(f -> collectmetas!(f.schema, metas, named, identities), s.fields)
    return nothing
end

function checkpublicname(name::AbstractString, what::String)
    isvalidname(name) || throw(ArgumentError("invalid $what \"$name\" (must match [A-Za-z_][A-Za-z0-9_]*)"))
    return nothing
end

function publicnamed(name::AbstractString, namespace::AbstractString, aliases)
    full = resolvefullname(name, nothing, namespace)
    checkpublicname(full.name, "name")
    isvalidnamespace(full.namespace) || throw(ArgumentError("invalid namespace \"$(full.namespace)\""))
    isreservedfullname(full) && throw(ArgumentError("\"$(full.name)\" is a primitive type name and cannot be redefined in the null namespace"))
    raw = publicaliases(aliases)
    return full, normalizedaliases(raw, full), raw
end

function publicaliases(aliases)
    return String[a isa Union{AbstractString,Symbol} ? String(a) : throw(ArgumentError("aliases must be strings")) for a in aliases]
end

"""
    Avro.FixedSchema(name, size; namespace="", logical=nothing, aliases=String[], props=(;))
"""
function FixedSchema(name::AbstractString, size::Integer; namespace::AbstractString="", logical=nothing, aliases=String[], props=(;))
    size >= 0 || throw(ArgumentError("fixed size must be ≥ 0"))
    full, norm, raw = publicnamed(name, namespace, aliases)
    p = makeprops(props, SCHEMA_GRAMMAR[:fixed], logical)
    return finalizepublic!(FixedSchema(full, norm, raw, Int(size), evaluatelogical(:fixed, Int(size), p), p, NodeMeta()))
end

"""
    Avro.EnumSchema(name, symbols; namespace="", default=Avro.nodefault, aliases=String[], doc=nothing, props=(;))
"""
function EnumSchema(name::AbstractString, symbols; namespace::AbstractString="", default=nodefault, aliases=String[], doc=nothing, props=(;))
    full, norm, raw = publicnamed(name, namespace, aliases)
    p = makeprops(props, SCHEMA_GRAMMAR[:enum], nothing)
    syms = String[]
    index = Dict{String,Int}()
    for x in symbols
        x isa Union{AbstractString,Symbol} || throw(ArgumentError("enum symbols must be strings"))
        sym = String(x)
        checkpublicname(sym, "enum symbol")
        haskey(index, sym) && throw(ArgumentError("duplicate enum symbol \"$sym\""))
        push!(syms, sym)
        index[sym] = length(syms)
    end
    d = nodefault
    if !(default isa NoDefault)
        default isa AbstractString && haskey(index, default) || throw(ArgumentError("enum default must be one of the symbols"))
        d = DefaultValue(String(default), 0, index[default], true)
    end
    s = EnumSchema(full, norm, raw, doc === nothing ? nothing : String(doc), syms, d, index, p, NodeMeta())
    return finalizepublic!(s)
end

"""
    Avro.ArraySchema(items; props=(;)) / Avro.MapSchema(values; props=(;))
"""
function ArraySchema(items::Schema; props=(;))
    return finalizepublic!(ArraySchema(items, makeprops(props, SCHEMA_GRAMMAR[:array], nothing), NodeMeta()))
end

function MapSchema(values::Schema; props=(;))
    return finalizepublic!(MapSchema(values, makeprops(props, SCHEMA_GRAMMAR[:map], nothing), NodeMeta()))
end

"""
    Avro.UnionSchema(branches)
"""
function UnionSchema(branches)
    bs = Schema[]
    seen = Set{Tuple{Bool,String}}()
    for branch in branches
        branch isa Schema || throw(ArgumentError("union branches must be schemas"))
        branch isa UnionSchema && throw(ArgumentError("unions may not immediately contain other unions"))
        identity = branchidentity(branch)
        identity in seen && throw(ArgumentError("duplicate union branch $(branchidentitylabel(branch))"))
        push!(seen, identity)
        push!(bs, branch)
    end
    return finalizepublic!(UnionSchema(bs, NodeMeta()))
end

"""
    Avro.Field(name, schema; default=Avro.nodefault, order=:ascending, aliases=String[], doc=nothing, props=(;))

A record field; `default` is a Julia value converted to JSON (`nothing`/`missing` → `null`) and
validated against `schema` with the recursive default rule.
"""
function Field(name::AbstractString, schema::Schema; default=nodefault, order::Symbol=:ascending, aliases=String[], doc=nothing, props=(;))
    checkpublicname(name, "field name")
    order in (:ascending, :descending, :ignore) || throw(ArgumentError("order must be :ascending, :descending or :ignore"))
    raw = publicaliases(aliases)
    d = nodefault
    if !(default isa NoDefault)
        j = tojsonvalue(default === missing ? nothing : default)
        ok, branch = validatedefault(schema, j)
        ok || throw(ArgumentError("default value for field \"$name\" does not match its schema"))
        d = DefaultValue(j, branch, 0, true)
    end
    return Field(String(name), schema, doc === nothing ? nothing : String(doc), d, order, fieldaliases(raw, name),
                 makeprops(props, FIELD_GRAMMAR, nothing))
end

"""
    Avro.RecordSchema(name; namespace="", fields=Avro.Field[], aliases=String[], doc=nothing, iserror=false, props=(;))
    Avro.RecordSchema(f, name; kw...)

A record; the second form builds a recursive record: `f(ref)` runs with the record registered but
unfilled and returns its fields (an inner `RecordSchema(g, …)` may use outer refs); if `f` throws, the
partial graph is discarded.
"""
function RecordSchema(name::AbstractString; namespace::AbstractString="", fields=Field[], aliases=String[], doc=nothing, iserror::Bool=false, props=(;))
    return RecordSchema(_ -> fields, name; namespace=namespace, aliases=aliases, doc=doc, iserror=iserror, props=props)
end

function RecordSchema(f, name::AbstractString; namespace::AbstractString="", aliases=String[], doc=nothing, iserror::Bool=false, props=(;))
    full, norm, raw = publicnamed(name, namespace, aliases)
    rec = RecordSchema(full, norm, raw, doc === nothing ? nothing : String(doc), iserror, makeprops(props, SCHEMA_GRAMMAR[:record], nothing), NodeMeta())
    rec.meta.id = FILLING
    for fld in f(rec)
        fld isa Field || throw(ArgumentError("fields must be Avro.Field values"))
        push!(rec.fields, fld)
    end
    rec.meta.id = Int32(-1)
    indexfields!(rec, (i, msg, key) -> throw(ArgumentError(msg)))
    return finalizepublic!(rec)
end

# Named nodes are copied once per `memo` (references stay references); unnamed nodes form a tree
# and are copied per occurrence.
function deepcopyschema(s::Schema, memo::IdDict{Schema,Schema})
    s isa NamedSchema && haskey(memo, s) && return memo[s]
    meta = NodeMeta()
    s isa Union{IntSchema,LongSchema,BytesSchema,StringSchema} && return typeof(s)(s.logical, s.props, meta)
    s isa Union{NullSchema,BooleanSchema,FloatSchema,DoubleSchema} && return typeof(s)(s.props, meta)
    s isa ArraySchema && return ArraySchema(deepcopyschema(s.items, memo), s.props, meta)
    s isa MapSchema && return MapSchema(deepcopyschema(s.values, memo), s.props, meta)
    s isa UnionSchema && return UnionSchema(Schema[deepcopyschema(b, memo) for b in s.branches], meta)
    s isa FixedSchema && return (memo[s] = FixedSchema(s.name, s.aliases, s.rawaliases, s.size, s.logical, s.props, meta))
    s isa EnumSchema && return (memo[s] = EnumSchema(s.name, s.aliases, s.rawaliases, s.doc, s.symbols, s.default, s.symbolindex, s.props, meta))
    rec = RecordSchema(s.name, s.aliases, s.rawaliases, s.doc, s.iserror, s.props, meta)
    memo[s] = rec                                  # before the fields, so self-references resolve to the copy
    for f in s.fields
        push!(rec.fields, Field(f.name, deepcopyschema(f.schema, memo), f.doc, f.default, f.order, f.aliases, f.props))
    end
    append!(rec.fieldnames, s.fieldnames)
    merge!(rec.fieldindex, s.fieldindex)
    return rec
end

"""
    projectrecord(s, positions) -> RecordSchema

The record `s` reduced to the fields at `positions`, in that order: a copy of its graph in which every
reference to `s` resolves to the new root. `s` was validated when it was built, so the copy skips the
constructors' rules and keeps repaired names and defaults.
"""
function projectrecord(s::RecordSchema, positions::Vector{Int})
    rec = RecordSchema(s.name, s.aliases, s.rawaliases, s.doc, s.iserror, s.props, NodeMeta())
    memo = IdDict{Schema,Schema}(s => rec)
    for i in positions
        f = s.fields[i]
        push!(rec.fields, Field(f.name, deepcopyschema(f.schema, memo), f.doc, f.default, f.order, f.aliases, f.props))
    end
    indexfields!(rec, (i, msg, key) -> throw(ArgumentError(msg)))
    metas = NodeMeta[]
    named = Dict{String,Schema}()
    collectmetas!(rec, metas, named, Dict{String,Schema}())
    gi = graphinfo(s)
    return sealgraph!(rec, metas, GraphInfo(gi.repaired_names, gi.repaired_defaults, length(metas), length(named)))
end

# ---- minsize -------------------------------------------------------------------------------------------

"""
    minsize(schema) -> Int

The minimal encoded size in bytes of a datum of `schema` (`typemax(Int)` when no finite datum exists,
e.g. an empty union, an empty enum, or a required recursive cycle without a nullable/array/map escape).
"""
function minsize(s::Schema)
    return nodemeta(s).minsize
end

# A fixed point from above: after k passes a node holds the least size of any datum of nesting ≤ k, so the
# iteration stops once every node has reached its least finite datum or stayed infinite. Children follow
# their parents in `reached`, so walking it backwards settles everything but cycles in one pass.
function computeminsizes!(root::Schema, nodes::Int)
    reached = Schema[]
    collectnodes!(reached, root, falses(nodes))
    best = fill(INFINITE, nodes)
    changed = true
    while changed
        changed = false
        for x in Iterators.reverse(reached)
            size = minsizeof(x, best)
            size < best[nodeid(x) + 1] || continue
            best[nodeid(x) + 1] = size
            changed = true
        end
    end
    for x in reached
        nodemeta(x).minsize = best[nodeid(x) + 1]
    end
    return nothing
end

function collectnodes!(nodes::Vector{Schema}, s::Schema, seen::BitVector)
    seen[nodeid(s) + 1] && return nothing
    seen[nodeid(s) + 1] = true
    push!(nodes, s)
    s isa ArraySchema && collectnodes!(nodes, s.items, seen)
    s isa MapSchema && collectnodes!(nodes, s.values, seen)
    s isa UnionSchema && foreach(b -> collectnodes!(nodes, b, seen), s.branches)
    s isa RecordSchema && foreach(f -> collectnodes!(nodes, f.schema, seen), s.fields)
    return nothing
end

# The least encoded size of `s` given `best`, the least sizes found so far for its children; one `isa`
# chain, so the fixed point over a graph's nodes dispatches statically.
function minsizeof(s::Schema, best::Vector{Int})
    if s isa RecordSchema
        total = 0
        for f in s.fields
            total = satadd(total, best[nodeid(f.schema) + 1])
        end
        return total
    elseif s isa UnionSchema
        least = INFINITE
        for (i, b) in enumerate(s.branches)
            least = min(least, satadd(best[nodeid(b) + 1], varintlength(i - 1)))
        end
        return least
    end
    return s isa NullSchema ? 0 :
        s isa FloatSchema ? 4 :
        s isa DoubleSchema ? 8 :
        s isa FixedSchema ? s.size :
        s isa EnumSchema ? (isempty(s.symbols) ? INFINITE : 1) :
        1                                       # boolean, int, long, bytes, string, and an empty array or map
end
