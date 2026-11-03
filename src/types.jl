# Julia type → schema derivation with the complete name policy, and the value-level `Avro.schema(x)`.

using StructUtils: StructUtils

"""
    Avro.AvroStyle <: StructUtils.StructStyle

The StructUtils style used by typed decoding and by `Avro.schema(T)` to read field tags and defaults.
"""
struct AvroStyle <: StructUtils.StructStyle end

"""
    Avro.avroname(::Type{T}) -> (name::String, namespace::String)

The Avro name and namespace derived for a Julia type (overridable). Default: `nameof(T)` with the
module path as namespace; parametric types append `_` and the sanitised parameter spelling
(`Box{Int64}` → `Box_Int64`, `Dict{String,Vector{Int64}}` → `Dict_String_Vector_Int64`; names over 128
bytes keep their first 100 characters and append `_` plus 16 hex digits of the SHA-256 of `string(T)`).
Deriving the parameter spelling needs run-time type printing, which `juliac --trim` does not compile. A
trimmed program overrides `avroname` for the parametric types it derives schemas for.
"""
function avroname(::Type{T}) where {T}
    base = string(nameof(T))
    params = T isa DataType ? T.parameters : ()
    name = isempty(params) ? base : string(base, "_", sanitizeparams(params))
    if sizeof(name) > 128
        digest = bytes2hex(SHA.sha256(string(T)))[1:16]
        name = string(first(name, 100), "_", digest)
    end
    return (name, modulepath(parentmodule(T)))
end

function sanitizeparams(params)
    return join((sanitize(string(p)) for p in params), "_")
end

function sanitize(s::String)
    io = IOBuffer()
    prevus = false
    for c in s
        ok = (c == '_') || ('A' <= c <= 'Z') || ('a' <= c <= 'z') || ('0' <= c <= '9')
        if ok && c != '_'
            Base.write(io, c)
            prevus = false
        elseif !prevus
            Base.write(io, '_')
            prevus = true
        end
    end
    out = strip(String(take!(io)), '_')
    isempty(out) && return "_"
    return isdigit(out[1]) ? string("_", out) : String(out)
end

function modulepath(m::Module)
    io = IOBuffer()
    dot = false
    for part in Base.fullname(m)
        dot && print(io, '.')
        print(io, part)
        dot = true
    end
    return String(take!(io))
end

"""
    Avro.avrosymbol(::Type{E}, x::E) -> String

The enum symbol emitted for the `Base.Enum` instance `x` (overridable; default `string(x)`).
"""
function avrosymbol(::Type{E}, x::E) where {E<:Base.Enum}
    return string(x)
end

"A bounded spelling of a type for error messages: long parameter lists keep 100 characters plus a digest."
function typedescription(@nospecialize(T::Type))
    s = string(T)
    sizeof(s) <= 128 && return s
    return string(first(s, 100), "#", bytes2hex(SHA.sha256(s))[1:16])
end

struct DeriveContext
    named::Dict{String,Schema}          # fullname → schema (a second Julia type deriving it is an error)
    origins::Dict{String,Type}          # fullname → Julia type that defined it
    derived::IdDict{Any,Schema}         # Julia type → its named schema
    anonymous::Base.RefValue{Int}       # counter for nested NamedTuple records
end

function DeriveContext(anonymous::Int)
    return DeriveContext(Dict{String,Schema}(), Dict{String,Type}(), IdDict{Any,Schema}(), Ref(anonymous))
end

function nameerror(what, remedy)
    throw(ArgumentError("invalid Avro $what; $remedy"))
end

function checkderivedname(name, what::String, remedy::String)
    name isa Union{AbstractString,Symbol} || nameerror(what, remedy)
    s = String(name)
    isvalidname(s) || nameerror(what, remedy)
    return s
end

function checkderivednamespace(ns)
    ns isa AbstractString && isvalidnamespace(ns) || nameerror("namespace", "override `Avro.avroname` for this type")
    return String(ns)
end

"""
    Avro.schema(T::Type; name=nothing, namespace=nothing) -> Schema

The conventional schema of a Julia type: `Missing`/`Nothing → null`, `Bool`, `Int8/16/32`,
`UInt8/16 → int`, `Int64`, `UInt32/64 → long`, `Float16/32 → float`, `Float64 → double`,
byte vectors `→ bytes`, strings/`Symbol`/`Char → string`, `NTuple{N,UInt8} → fixed_N`,
`Union{Missing,T} → ["null", T]`, other unions in `Base.uniontypes` order, `Base.Enum` subtypes → enum,
`DateTime → local-timestamp-millis`, `Date → date`, `Time → time-micros`, `UUID → string uuid`,
`Avro.Timestamp{P}`/`Avro.LocalTimestamp{P}`, `Avro.Duration → fixed(12) duration`, vectors → array,
string-keyed dicts → map, `NamedTuple`s and structs → records. Every derived name must already be a
valid Avro name (no transliteration); `name=`/`namespace=` override the root.
"""
function schema(::Type{T}; name=nothing, namespace=nothing) where {T}
    ctx = DeriveContext(0)
    return finalizepublic!(derive(ctx, T, name, namespace))
end

# The `long` logical type of a timestamp type, or `nothing`. Every concrete `Durations.ZonedTimestamp` is an
# instant. The result is a constant for a constant `T`.
function timestamplogical(::Type{T}) where {T}
    T === LocalTimestamp{Millisecond} && return LocalTimestampMillis()
    T === LocalTimestamp{Microsecond} && return LocalTimestampMicros()
    T === LocalTimestamp{Nanosecond} && return LocalTimestampNanos()
    (T <: Durations.ZonedTimestamp && isconcretetype(T)) || return nothing
    P = T.parameters[1]
    P === Millisecond && return TimestampMillis()
    P === Microsecond && return TimestampMicros()
    P === Nanosecond && return TimestampNanos()
    return nothing
end

# Calls `f(name, FT)` for each field of `T` in declaration order, with the field type as a constant. A loop
# over `fieldtypes(T)` would derive each field's schema from a type known only at run time.
@generated function foreachfield(f::F, ::Type{T}) where {F,T}
    calls = [:(f($(QuoteNode(fieldname(T, i))), $(fieldtype(T, i)))) for i in 1:fieldcount(T)]
    return Expr(:block, calls..., nothing)
end

# Calls `f(i, M)` for the `i`th member type `M` of the union `U` in `Base.uniontypes` order, with `M` as a
# constant.
@generated function foreachuniontype(f::F, ::Type{U}) where {F,U}
    calls = [:(f($i, $M)) for (i, M) in enumerate(Base.uniontypes(U))]
    return Expr(:block, calls..., nothing)
end

function derive(ctx::DeriveContext, ::Type{T}, name, namespace) where {T}
    existing = get(ctx.derived, T, nothing)
    existing === nothing || return existing
    return deriveimpl(ctx, T, name, namespace)
end

function logicalprimitive(::Type{S}, logical::LogicalType) where {S<:PrimitiveSchema}
    return S(logical, makeprops((;), ("type",), logical), NodeMeta())
end

function deriveimpl(ctx::DeriveContext, ::Type{T}, name, namespace) where {T}
    T === Union{} && throw(ArgumentError("no conventional Avro schema for Union{}; pass an explicit `schema=`"))
    (T === Missing || T === Nothing) && return NullSchema(Props(), NodeMeta())
    T === Bool && return BooleanSchema(Props(), NodeMeta())
    T in (Int8, Int16, Int32, UInt8, UInt16) && return IntSchema(nothing, Props(), NodeMeta())
    T in (Int64, UInt32, UInt64) && return LongSchema(nothing, Props(), NodeMeta())
    T in (Float16, Float32) && return FloatSchema(Props(), NodeMeta())
    T === Float64 && return DoubleSchema(Props(), NodeMeta())
    T <: AbstractVector{UInt8} && return BytesSchema(nothing, Props(), NodeMeta())
    (T <: AbstractString || T === Symbol || T === Char) && return StringSchema(nothing, Props(), NodeMeta())
    T === UUID && return logicalprimitive(StringSchema, UUIDLogical())
    T === Date && return logicalprimitive(IntSchema, DateLogical())
    T === Time && return logicalprimitive(LongSchema, TimeMicros())
    T === DateTime && return logicalprimitive(LongSchema, LocalTimestampMillis())
    tl = timestamplogical(T)
    tl === nothing || return logicalprimitive(LongSchema, tl)
    T <: DataDecimals.Decimal && return BytesSchema(; logical=DecimalLogical(precision(T), DataDecimals.scale(T)))
    (T === Durations.Duration || T === Duration) &&
        return namedfixed(ctx, "Duration", namespace === nothing ? "" : namespace, 12, DurationLogical(), T)
    (T <: DataDecimals.DecimalValue || T === WideDecimal) &&
        throw(ArgumentError("a decimal needs a precision and scale: pass an explicit `schema=` (e.g. `Avro.BytesSchema(; logical=Avro.DecimalLogical(p, s))`)"))
    T === UnionValue && throw(ArgumentError("a bare Avro.UnionValue has no conventional schema: use `Avro.encode(schema, x)`"))
    T === Record && throw(ArgumentError("each Avro.Record carries its own schema, so its type has no conventional one; pass an explicit `schema=`"))
    T isa Union && return deriveunion(ctx, T, namespace)
    if T <: NTuple{N,UInt8} where {N}
        n = length(T.parameters)
        return namedfixed(ctx, "fixed_$n", namespace === nothing ? "" : namespace, n, nothing, T)
    end
    T <: Base.Enum && return deriveenum(ctx, T, name, namespace)
    if T <: AbstractDict
        K, V = keytype(T), valtype(T)
        (K <: AbstractString || K === Symbol) ||
            throw(ArgumentError("Avro maps have string keys; cannot derive a schema from $(typedescription(T))"))
        return MapSchema(derive(ctx, V, nothing, namespace), Props(), NodeMeta())
    end
    T <: AbstractVector && return ArraySchema(derive(ctx, eltype(T), nothing, namespace), Props(), NodeMeta())
    T <: NamedTuple && return derivenamedtuple(ctx, T, name, namespace)
    T <: Tuple && throw(ArgumentError("tuples other than NTuple{N,UInt8} have no conventional schema; use a NamedTuple or struct"))
    isstructtype(T) && isconcretetype(T) && return derivestruct(ctx, T, name, namespace)
    throw(ArgumentError("no conventional Avro schema for $(typedescription(T)); pass an explicit `schema=`"))
end

function registerderived!(ctx::DeriveContext, s::NamedSchema, ::Type{T}) where {T}
    full = fullname(s)
    existing = get(ctx.named, full, nothing)
    if existing !== nothing
        origin = ctx.origins[full]
        origin === T && return existing
        throw(ArgumentError("Julia types $(typedescription(origin)) and $(typedescription(T)) both derive the Avro fullname \"$full\"; override `Avro.avroname` for one of them"))
    end
    ctx.named[full] = s
    ctx.origins[full] = T
    ctx.derived[T] = s
    return s
end

function namedfixed(ctx::DeriveContext, name::AbstractString, namespace::AbstractString, size::Int, logical, ::Type{T}) where {T}
    full = FullName(checkderivedname(name, "name", "pass an explicit schema"), checkderivednamespace(namespace))
    existing = get(ctx.named, fullname(full), nothing)
    existing === nothing || return registerderived!(ctx, existing::NamedSchema, T)
    p = makeprops((;), SCHEMA_GRAMMAR[:fixed], logical)
    return registerderived!(ctx, FixedSchema(full, String[], String[], size, logical, p, NodeMeta()), T)
end

function derivedname(::Type{T}, name, namespace) where {T}
    if name === nothing || namespace === nothing
        defaultname, defaultnamespace = avroname(T)
        name === nothing && (name = defaultname)
        namespace === nothing && (namespace = defaultnamespace)
    end
    remedy = "override `Avro.avroname` for this type or pass root `name=` and `namespace=`"
    return FullName(checkderivedname(name, "name", remedy), checkderivednamespace(namespace))
end

function deriveenum(ctx::DeriveContext, ::Type{E}, name, namespace) where {E<:Base.Enum}
    full = derivedname(E, name, namespace)
    existing = get(ctx.named, fullname(full), nothing)
    existing === nothing || return registerderived!(ctx, existing::NamedSchema, E)
    syms = String[]
    index = Dict{String,Int}()
    for inst in instances(E)
        sym = checkderivedname(avrosymbol(E, inst), "enum symbol", "override `Avro.avrosymbol` for this enum")
        haskey(index, sym) && throw(ArgumentError("enum $(typedescription(E)) derives the symbol \"$sym\" twice"))
        push!(syms, sym)
        index[sym] = length(syms)
    end
    s = EnumSchema(full, String[], String[], nothing, syms, nodefault, index, Props(), NodeMeta())
    return registerderived!(ctx, s, E)
end

function deriveunion(ctx::DeriveContext, ::Type{U}, namespace) where {U}
    members = Base.uniontypes(U)
    if length(members) == 2 && any(m -> m === Missing, members) && !any(m -> m === Nothing, members)
        return UnionSchema(Schema[derive(ctx, Missing, nothing, namespace), derive(ctx, Base.nonmissingtype(U), nothing, namespace)], NodeMeta())
    end
    bs = Schema[]
    seen = Dict{Tuple{Bool,String},Int}()
    branch! = function (i::Int, ::Type{M}) where {M}
        s = derive(ctx, M, nothing, namespace)
        s isa UnionSchema && throw(ArgumentError("union member $(typedescription(M)) derives a union; Avro unions cannot nest"))
        identity = branchidentity(s)
        previous = get(seen, identity, 0)
        previous == 0 || throw(ArgumentError("union members $(typedescription(members[previous]::Type)) and $(typedescription(M)) both map to the Avro branch \"$(branchidentitylabel(s))\"; Avro unions cannot repeat a kind — use a named type or an explicit `schema=`"))
        seen[identity] = i
        push!(bs, s)
        return nothing
    end
    foreachuniontype(branch!, U)
    return UnionSchema(bs, NodeMeta())
end

function derivenamedtuple(ctx::DeriveContext, ::Type{T}, name, namespace) where {T<:NamedTuple}
    if name === nothing
        ctx.anonymous[] += 1
        name = ctx.anonymous[] == 1 ? "Record" : "Record_$(ctx.anonymous[] - 1)"
    end
    full = FullName(checkderivedname(name, "name", "pass a valid `name=`"), checkderivednamespace(namespace === nothing ? "" : namespace))
    haskey(ctx.named, fullname(full)) && throw(ArgumentError("the Avro name \"$(fullname(full))\" is derived twice"))
    rec = RecordSchema(full, String[], String[], nothing, false, Props(), NodeMeta())
    registerderived!(ctx, rec, T)
    field! = function (fname::Symbol, ::Type{FT}) where {FT}
        fn = checkderivedname(fname, "field name", "rename the field")
        push!(rec.fields, Field(fn, derive(ctx, FT, nothing, full.namespace), nothing, nodefault, :ascending, String[], Props()))
        return nothing
    end
    foreachfield(field!, T)
    return indexfields!(rec, (i, msg, key) -> throw(ArgumentError(msg)))
end

"The `key` entry (`:name` or `:default`) of a field's `avro=(…)` tag; `:name` also honours a plain `name=` tag."
function fieldtag(tags, field::Symbol, key::Symbol)
    t = get(tags, field, nothing)
    t === nothing && return nothing
    a = get(t, :avro, nothing)
    a !== nothing && haskey(a, key) && return a[key]
    key === :name && haskey(t, :name) && return t[:name]
    return nothing
end

function derivestruct(ctx::DeriveContext, ::Type{T}, name, namespace) where {T}
    full = derivedname(T, name, namespace)
    existing = get(ctx.named, fullname(full), nothing)
    existing === nothing || return registerderived!(ctx, existing::NamedSchema, T)
    tags = StructUtils.fieldtags(AvroStyle(), T)
    defaults = StructUtils.fielddefaults(AvroStyle(), T)
    rec = RecordSchema(full, String[], String[], nothing, false, Props(), NodeMeta())
    registerderived!(ctx, rec, T)
    field! = function (fname::Symbol, ::Type{FT}) where {FT}
        tagged = fieldtag(tags, fname, :name)
        fn = checkderivedname(tagged === nothing ? fname : tagged, "field name", "tag the field with `&(avro=(name=\"…\",),)`")
        fs = derive(ctx, FT, nothing, full.namespace)
        d = structdefault(T, fname, fs, fieldtag(tags, fname, :default), defaults)
        push!(rec.fields, Field(fn, fs, nothing, d, :ascending, String[], Props()))
        return nothing
    end
    foreachfield(field!, T)
    return indexfields!(rec, (i, msg, key) -> throw(ArgumentError(msg)))
end

"A struct field's default: the `avro=(default=…,)` tag first, then the StructUtils field default."
function structdefault(::Type{T}, fname::Symbol, @nospecialize(fs::Schema), tagdefault, defaults) where {T}
    if tagdefault !== nothing
        j = tojsonvalue(tagdefault)
        ok, branch = validatedefault(fs, j)
        ok || throw(ArgumentError("the `avro=(default=…,)` tag of $(typedescription(T)).\"$fname\" is not a valid default for its schema"))
        return DefaultValue(j, branch, 0, true)
    end
    haskey(defaults, fname) || return nodefault
    dv = defaults[fname]
    msg = "the default of $(typedescription(T)).\"$fname\" is not JSON-encodable under its schema; supply one with the `&(avro=(default=…,),)` tag"
    j = try
        tojsonvalue(dv === missing ? nothing : dv)
    catch err
        err isa ArgumentError || rethrow()
        throw(ArgumentError(msg))
    end
    ok, branch = validatedefault(fs, j)
    ok || throw(ArgumentError(msg))
    return DefaultValue(j, branch, 0, true)
end

"""
    Avro.schema(::Tables.Schema; name="Record", namespace="") -> Schema

A record schema from a `Tables.Schema` (column names and types).
"""
function schema(@nospecialize(ts::Tables.Schema); name::AbstractString="Record", namespace::AbstractString="")
    ctx = DeriveContext(1)
    full = FullName(checkderivedname(name, "name", "pass a valid `name=`"), checkderivednamespace(namespace))
    rec = RecordSchema(full, String[], String[], nothing, false, Props(), NodeMeta())
    registerderived!(ctx, rec, Tables.Schema)
    for (col, ct) in zip(ts.names, ts.types)
        fn = checkderivedname(col, "column name", "rename the column")
        push!(rec.fields, Field(fn, derivecolumn(ctx, col, ct, full.namespace), nothing, nodefault, :ascending, String[], Props()))
    end
    indexfields!(rec, (i, msg, key) -> throw(ArgumentError(msg)))
    return finalizepublic!(rec)
end

function derivecolumn(ctx::DeriveContext, column, @nospecialize(T::Type), namespace)
    try
        return derive(ctx, T, nothing, namespace)
    catch err
        err isa ArgumentError || rethrow()
        throw(ArgumentError("column $column of type $(typedescription(T)): $(err.msg)"))
    end
end

"""
    Avro.schema(x) -> Schema

The schema of a value: identity-bearing generic values (`Record`, `EnumValue`, `Fixed`) return their
instance schema; vectors/maps of identity-bearing values of one identity (including `Vector{Any}` and
`Dict{String,Any}`) return the array/map of it; other values use the conventional `Avro.schema(typeof(x))`.
Mixed identities, plain values in an untyped collection, empty identity-bearing or untyped collections, a
bare `UnionValue` and a `Decimal`/`WideDecimal` raise `ArgumentError` (use `Avro.encode(schema, x)`).
"""
function schema(x::Record)
    return getfield(x, :schema)
end

function schema(x::EnumValue)
    return x.schema
end

function schema(x::Fixed)
    return x.schema
end

function schema(::UnionValue)
    throw(ArgumentError("a bare Avro.UnionValue has no schema of its own: use `Avro.encode(schema, x)`"))
end

function schema(::Union{Decimal,WideDecimal})
    throw(ArgumentError("a decimal needs a precision and scale: use `Avro.encode(schema, x)`"))
end

const IDENTITY_VALUES = Union{Record,EnumValue,Fixed}

function schema(x::AbstractVector{T}) where {T}
    (T <: IDENTITY_VALUES || T === Any) || return schema(typeof(x))
    return ArraySchema(uniformschema(x))
end

function schema(x::AbstractDict{K,V}) where {K,V}
    (K <: AbstractString || K === Symbol) && (V <: IDENTITY_VALUES || V === Any) || return schema(typeof(x))
    return MapSchema(uniformschema(values(x)))
end

function uniformschema(xs)
    isempty(xs) && throw(ArgumentError("an empty collection of identity-bearing values has no schema of its own: use `Avro.encode(schema, x)`"))
    all(v -> v isa IDENTITY_VALUES, xs) ||
        throw(ArgumentError("a collection mixing identity-bearing and plain values has no schema of its own: use `Avro.encode(schema, x)`"))
    s = schema(first(xs))
    all(v -> schema(v).name == s.name, xs) ||
        throw(ArgumentError("a collection of values with different schema identities has no schema of its own: use `Avro.encode(schema, x)`"))
    return s
end

function schema(x)
    return schema(typeof(x))
end
