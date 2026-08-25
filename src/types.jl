# Julia type → schema derivation with the complete name policy (plan §4.8), and the value-level
# `Avro.schema(x)`.

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
characters keep their first 100 and append `_` plus 16 hex digits of the SHA-256 of `string(T)`).
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
    parts = String[]
    for p in params
        push!(parts, sanitize(string(p)))
    end
    return join(parts, "_")
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
    out = String(take!(io))
    out = strip(out, '_')
    isempty(out) && return "_"
    isdigit(out[1]) && (out = "_" * out)
    return out
end

function modulepath(m::Module)
    return join(string.(Base.fullname(m)), ".")
end

"""
    Avro.avrosymbol(::Type{E}, x::E) -> String

The enum symbol emitted for the `Base.Enum` instance `x` (overridable; default `string(x)`).
"""
function avrosymbol(::Type{E}, x::E) where {E<:Base.Enum}
    return string(x)
end

struct DeriveContext
    limits::Limits
    budget::Budget
    named::FrozenDict{String,Schema}          # fullname → schema (first definition wins)
    origins::FrozenDict{String,Type}          # fullname → Julia type that defined it
    anonymous::Base.RefValue{Int}             # counter for nested NamedTuple records
end

function nameerror(what, julia, remedy)
    throw(ArgumentError("invalid Avro $what derived from $julia; $remedy"))
end

function checkderivedname(name::AbstractString, what::String, julia::String, remedy::String)
    isvalidname(name) || nameerror(what, julia, remedy)
    return String(name)
end

function checkderivednamespace(ns::AbstractString, julia::String)
    isvalidnamespace(ns) || nameerror("namespace \"$ns\"", julia, "override `Avro.avroname(::Type{$julia})`")
    return String(ns)
end

"""
    Avro.schema(T::Type; name=nothing, namespace=nothing, limits=Limits()) -> Schema

The conventional schema of a Julia type (plan §4.8): `Missing`/`Nothing → null`, `Bool`, `Int8/16/32`,
`UInt8/16 → int`, `Int64`, `UInt32/64 → long`, `Float16/32 → float`, `Float64 → double`,
`Vector{UInt8} → bytes`, strings/`Symbol`/`Char → string`, `NTuple{N,UInt8} → fixed_N`,
`Union{Missing,T} → ["null", T]`, other unions in member order, `Base.Enum` subtypes → enum,
`DateTime → local-timestamp-millis`, `Date → date`, `Time → time-micros`, `UUID → string uuid`,
`Avro.Timestamp{P}`/`Avro.LocalTimestamp{P}`, `Avro.Duration → fixed(12) duration`, vectors → array,
`Avro.Map`/string-keyed dicts → map, `NamedTuple`s and structs → records. Every derived name must already
be a valid Avro name (no transliteration); `name=`/`namespace=` override the root.
"""
function schema(::Type{T}; name=nothing, namespace=nothing, limits::Limits=Limits()) where {T}
    return withconstruction(limits; direction=:encode) do budget   # finalize and defaults share this scope (D01)
        ctx = DeriveContext(limits, budget, FrozenDict{String,Schema}(), FrozenDict{String,Type}(), Ref(0))
        s = withbuilder(() -> derive(ctx, T, name === nothing ? nothing : String(name), namespace === nothing ? nothing : String(namespace)))
        finalizepublic!(s, limits, 0, 0)
    end
end

const LOGICAL_LONG = Dict{Type,LogicalType}(
    Timestamp{Millisecond} => TimestampMillis(), Timestamp{Microsecond} => TimestampMicros(), Timestamp{Nanosecond} => TimestampNanos(),
    LocalTimestamp{Millisecond} => LocalTimestampMillis(), LocalTimestamp{Microsecond} => LocalTimestampMicros(),
    LocalTimestamp{Nanosecond} => LocalTimestampNanos(),
)

function derive(ctx::DeriveContext, ::Type{T}, name, namespace) where {T}
    reserve!(ctx.budget, 160)                          # the derived node and its meta, settled once built
    s = deriveimpl(ctx, T, name, namespace)
    allocated!(ctx.budget, 160)
    return s
end

function deriveimpl(ctx::DeriveContext, ::Type{T}, name, namespace) where {T}
    T === Missing && return NullSchema(Props(), NodeMeta())
    T === Nothing && return NullSchema(Props(), NodeMeta())
    T === Bool && return BooleanSchema(Props(), NodeMeta())
    T in (Int8, Int16, Int32, UInt8, UInt16) && return IntSchema(nothing, Props(), NodeMeta())
    T in (Int64, UInt32, UInt64) && return LongSchema(nothing, Props(), NodeMeta())
    T in (Float16, Float32) && return FloatSchema(Props(), NodeMeta())
    T === Float64 && return DoubleSchema(Props(), NodeMeta())
    T === Vector{UInt8} && return BytesSchema(nothing, Props(), NodeMeta())
    (T <: AbstractString || T === Symbol || T === Char) && return StringSchema(nothing, Props(), NodeMeta())
    T === UUID && return StringSchema(UUIDLogical(), makeprops((;), ("type",), UUIDLogical()), NodeMeta())
    T === Date && return IntSchema(DateLogical(), makeprops((;), ("type",), DateLogical()), NodeMeta())
    T === Time && return LongSchema(TimeMicros(), makeprops((;), ("type",), TimeMicros()), NodeMeta())
    T === DateTime && return LongSchema(LocalTimestampMillis(), makeprops((;), ("type",), LocalTimestampMillis()), NodeMeta())
    haskey(LOGICAL_LONG, T) && return LongSchema(LOGICAL_LONG[T], makeprops((;), ("type",), LOGICAL_LONG[T]), NodeMeta())
    T === Duration && return namedfixed(ctx, "Duration", namespace === nothing ? "" : namespace, 12, DurationLogical(), T)
    (T === Decimal || T === WideDecimal) && throw(ArgumentError("a decimal needs a precision and scale: pass an explicit `schema=` (e.g. `Avro.BytesSchema(; logical=Avro.DecimalLogical(p, s))`)"))
    T === UnionValue && throw(ArgumentError("a bare Avro.UnionValue has no conventional schema: use `Avro.encode(schema, x)`"))
    if T isa Union
        return deriveunion(ctx, T, namespace)
    end
    if T <: NTuple && T isa DataType && !isempty(T.parameters) && T.parameters[1] isa Tuple
        return derivetuple(ctx, T, namespace)
    end
    if T <: NTuple{N,UInt8} where {N}
        n = length(T.parameters)
        return namedfixed(ctx, "fixed_$n", namespace === nothing ? "" : namespace, n, nothing, T)
    end
    T <: Base.Enum && return deriveenum(ctx, T, name, namespace)
    T <: Map && return MapSchema(derive(ctx, eltype(T).parameters[2], nothing, namespace), Props(), NodeMeta())
    if T <: AbstractDict
        K, V = keytype(T), valtype(T)
        (K <: AbstractString || K === Symbol) || throw(ArgumentError("Avro maps have string keys; cannot derive a schema from $T"))
        return MapSchema(derive(ctx, V, nothing, namespace), Props(), NodeMeta())
    end
    T <: AbstractVector && return ArraySchema(derive(ctx, eltype(T), nothing, namespace), Props(), NodeMeta())
    T <: NamedTuple && return derivenamedtuple(ctx, T, name, namespace)
    T <: Tuple && throw(ArgumentError("tuples other than NTuple{N,UInt8} have no conventional schema; use a NamedTuple or struct"))
    isstructtype(T) && isconcretetype(T) && return derivestruct(ctx, T, name, namespace)
    throw(ArgumentError("no conventional Avro schema for $T; pass an explicit `schema=`"))
end

function registerderived!(ctx::DeriveContext, s::NamedSchema, ::Type{T}) where {T}
    full = fullname(s)
    if haskey(ctx.named, full)
        origin = ctx.origins[full]
        origin === T && return ctx.named[full]
        throw(ArgumentError("Julia types $origin and $T both derive the Avro fullname \"$full\"; override `Avro.avroname` for one of them"))
    end
    length(ctx.named) < ctx.limits.max_named_types || throw(LimitError(:max_named_types, length(ctx.named) + 1, ctx.limits.max_named_types, :max_named_types, :encode))
    ctx.named[full] = s
    ctx.origins[full] = T
    return s
end

function namedfixed(ctx::DeriveContext, name::String, namespace::String, size::Int, logical, ::Type{T}) where {T}
    full = FullName(name, namespace)
    haskey(ctx.named, fullname(full)) && return registerderived!(ctx, ctx.named[fullname(full)], T)
    p = makeprops((;), SCHEMA_GRAMMAR[:fixed], logical)
    s = FixedSchema(full, freeze!(FrozenVector{String}()), freeze!(FrozenVector{String}()), size, logical, p, NodeMeta())
    return registerderived!(ctx, s, T)
end

function derivedname(ctx::DeriveContext, ::Type{T}, name, namespace) where {T}
    n, ns = avroname(T)
    name === nothing || (n = name)
    namespace === nothing || (ns = namespace)
    julia = string(T)
    remedy = "override `Avro.avroname(::Type{$julia})` (or pass `name=`/`namespace=` for the root)"
    n = checkderivedname(n, "name \"$n\"", julia, remedy)
    ns = checkderivednamespace(ns, julia)
    return FullName(n, ns)
end

function deriveenum(ctx::DeriveContext, ::Type{E}, name, namespace) where {E<:Base.Enum}
    full = derivedname(ctx, E, name, namespace)
    haskey(ctx.named, fullname(full)) && return registerderived!(ctx, ctx.named[fullname(full)], E)
    n = length(instances(E))
    slots = 2 * frozenvectorshell() + frozendictshell() + vectorbytes(String, n) + vectorbytes(String, n) + vectorbytes(Int, n)
    reserve!(ctx.budget, slots)                        # shells and exact slot capacity (§4.4)
    syms = Vector{String}(undef, n)
    resize!(syms, 0)
    index = emptywithcapacity(FrozenDict{String,Int}, n)
    allocated!(ctx.budget, slots)
    for (i, inst) in enumerate(instances(E))
        sym = avrosymbol(E, inst)
        retain!(ctx.budget, stringbytes(sizeof(sym)))                     # the symbol string the schema keeps
        checkderivedname(sym, "enum symbol \"$sym\"", string(E), "override `Avro.avrosymbol(::Type{$E}, x)`")
        haskey(index, sym) && throw(ArgumentError("enum $E derives the symbol \"$sym\" twice"))
        push!(syms, sym)
        index[sym] = i
    end
    s = EnumSchema(full, freeze!(FrozenVector{String}()), freeze!(FrozenVector{String}()), nothing, freeze!(FrozenVector{String}(syms, false)), nodefault, freeze!(index), Props(), NodeMeta())
    return registerderived!(ctx, s, E)
end

function deriveunion(ctx::DeriveContext, ::Type{U}, namespace) where {U}
    members = Base.uniontypes(U)
    if length(members) == 2 && Missing in members && !(Nothing in members)
        other = members[1] === Missing ? members[2] : members[1]
        twobranch = frozenvectorshell() + 16
        reserve!(ctx.budget, twobranch)                # the two-branch vector, built next (§4.4)
        bs = FrozenVector{Schema}(Schema[NullSchema(Props(), NodeMeta()), derive(ctx, other, nothing, namespace)], false)
        allocated!(ctx.budget, twobranch)
        return UnionSchema(freeze!(bs), NodeMeta())
    end
    branchbytes = frozenvectorshell() + vectorbytes(Schema, length(members))
    reserve!(ctx.budget, branchbytes)                  # shell and exact branch capacity (§4.4)
    bs = emptywithcapacity(FrozenVector{Schema}, length(members))
    allocated!(ctx.budget, branchbytes)
    for m in members
        s = derive(ctx, m, nothing, namespace)
        s isa UnionSchema && throw(ArgumentError("union member $m derives a union; Avro unions cannot nest"))
        ident = branchidentity(s)
        for (j, prev) in enumerate(bs)
            branchidentity(prev) == ident && throw(ArgumentError("union members $(members[j]) and $m both map to the Avro branch \"$(ident[6:end])\"; Avro unions cannot repeat a kind — use a named type or an explicit `schema=`"))
        end
        push!(bs, s)
    end
    return UnionSchema(freeze!(bs), NodeMeta())
end

function derivetuple(ctx::DeriveContext, ::Type{T}, namespace) where {T}
    throw(ArgumentError("tuples other than NTuple{N,UInt8} have no conventional schema; use a NamedTuple or struct"))
end

function derivenamedtuple(ctx::DeriveContext, ::Type{T}, name, namespace) where {T<:NamedTuple}
    names = fieldnames(T)
    types = T.parameters[2].parameters
    if name === nothing
        ctx.anonymous[] += 1
        name = ctx.anonymous[] == 1 ? "Record" : "Record_$(ctx.anonymous[] - 1)"
    end
    ns = namespace === nothing ? "" : namespace
    full = FullName(checkderivedname(name, "name \"$name\"", string(T), "pass a valid `name=`"), checkderivednamespace(ns, string(T)))
    haskey(ctx.named, fullname(full)) && throw(ArgumentError("the Avro name \"$(fullname(full))\" is derived twice"))
    nf = length(names)
    slots = frozenvectorshell() + frozendictshell() + 128 + vectorbytes(Field, nf) + vectorbytes(String, nf) + vectorbytes(Int, nf)
    reserve!(ctx.budget, slots)                        # record shells and exact field capacity (§4.4)
    fields = emptywithcapacity(FrozenVector{Field}, nf)
    index = emptywithcapacity(FrozenDict{String,Int}, nf)
    allocated!(ctx.budget, slots)
    rec = RecordSchema(full, freeze!(FrozenVector{String}()), freeze!(FrozenVector{String}()), nothing, false, Props(), fields, index, NodeMeta())
    registerderived!(ctx, rec, T)
    for (i, (fname, ftype)) in enumerate(zip(names, types))
        fn = checkderivedname(string(fname), "field name \"$fname\"", string(T), "rename the field")
        fbytes = stringbytes(sizeof(fn)) + 2 * frozenvectorshell() + 128
        reserve!(ctx.budget, fbytes)                   # the field name, wrappers and Field shell, built next
        push!(fields, Field(fn, derive(ctx, ftype, nothing, full.namespace), nothing, nodefault, :ascending, freeze!(FrozenVector{String}()), Props()))
        allocated!(ctx.budget, fbytes)
        index[fn] = i
    end
    freeze!(fields)
    freeze!(index)
    return rec
end

function fieldtag(tags, field::Symbol, key::Symbol)
    return begin
        t = get(tags, field, nothing)
        t === nothing && return nothing
        a = get(t, :avro, nothing)
        a !== nothing && haskey(a, key) && return a[key]
        key === :name && haskey(t, :name) && return t[:name]
        return nothing
        end
end

function derivestruct(ctx::DeriveContext, ::Type{T}, name, namespace) where {T}
    full = derivedname(ctx, T, name, namespace)
    haskey(ctx.named, fullname(full)) && return registerderived!(ctx, ctx.named[fullname(full)], T)
    tags = StructUtils.fieldtags(AvroStyle(), T)
    defaults = StructUtils.fielddefaults(AvroStyle(), T)
    nf = fieldcount(T)
    slots = frozenvectorshell() + frozendictshell() + 128 + vectorbytes(Field, nf) + vectorbytes(String, nf) + vectorbytes(Int, nf)
    reserve!(ctx.budget, slots)                        # record shells and exact field capacity (§4.4)
    fields = emptywithcapacity(FrozenVector{Field}, nf)
    index = emptywithcapacity(FrozenDict{String,Int}, nf)
    allocated!(ctx.budget, slots)
    rec = RecordSchema(full, freeze!(FrozenVector{String}()), freeze!(FrozenVector{String}()), nothing, false, Props(), fields, index, NodeMeta())
    registerderived!(ctx, rec, T)
    for (i, fname) in enumerate(fieldnames(T))
        ftype = fieldtype(T, i)
        tagged = fieldtag(tags, fname, :name)
        fn = tagged === nothing ? string(fname) : String(tagged)
        checkderivedname(fn, "field name \"$fn\"", string(T, ".", fname), "tag the field with `&(avro=(name=\"…\",),)`")
        haskey(index, fn) && throw(ArgumentError("struct $T derives the field name \"$fn\" twice"))
        fs = derive(ctx, ftype, nothing, full.namespace)
        d = nodefault
        tagdefault = fieldtag(tags, fname, :default)
        if tagdefault !== nothing
            j = tojsonvalue(tagdefault, ctx.budget)
            ok, branch = validatedefault(fs, j, ctx.limits.max_depth)
            ok || throw(ArgumentError("the `avro=(default=…,)` tag of $T.$fname is not a valid default for its schema"))
            jw = BoundedWriter(ctx.budget, ctx.limits.max_schema_bytes)
            printjson(jw, j, false, 0)
            d = DefaultValue(j, branch, boundedtake!(jw), 0, true)
        elseif haskey(defaults, fname)
            dv = defaults[fname]
            j = try
                tojsonvalue(dv === missing ? nothing : dv, ctx.budget)
            catch
                throw(ArgumentError("the default of $T.$fname is not JSON-encodable under its schema; supply one with the `&(avro=(default=…,),)` tag"))
            end
            ok, branch = validatedefault(fs, j, ctx.limits.max_depth)
            ok || throw(ArgumentError("the default of $T.$fname is not JSON-encodable under its schema; supply one with the `&(avro=(default=…,),)` tag"))
            jw = BoundedWriter(ctx.budget, ctx.limits.max_schema_bytes)
            printjson(jw, j, false, 0)
            d = DefaultValue(j, branch, boundedtake!(jw), 0, true)
        end
        fbytes = stringbytes(sizeof(fn)) + 2 * frozenvectorshell() + 128
        reserve!(ctx.budget, fbytes)                   # the field name, wrappers and Field shell, built next
        push!(fields, Field(fn, fs, nothing, d, :ascending, freeze!(FrozenVector{String}()), Props()))
        allocated!(ctx.budget, fbytes)
        index[fn] = i
    end
    freeze!(fields)
    freeze!(index)
    return rec
end

"""
    Avro.schema(::Tables.Schema; name="Record", namespace="", names=Dict(), limits=Limits()) -> Schema

A record schema from a `Tables.Schema` (column names and types); `names` renames columns.
"""
function schema(ts::Tables.Schema; name::AbstractString="Record", namespace::AbstractString="", names=Dict{Symbol,String}(), limits::Limits=Limits())
    return withconstruction(limits; direction=:encode) do budget   # finalize and defaults share this scope (D01)
        ctx = DeriveContext(limits, budget, FrozenDict{String,Schema}(), FrozenDict{String,Type}(), Ref(1))
        s = withbuilder() do
            full = FullName(checkderivedname(name, "name \"$name\"", "Tables.Schema", "pass a valid `name=`"), checkderivednamespace(namespace, "Tables.Schema"))
            fields = FrozenVector{Field}()
            index = FrozenDict{String,Int}()
            rec = RecordSchema(full, freeze!(FrozenVector{String}()), freeze!(FrozenVector{String}()), nothing, false, Props(), fields, index, NodeMeta())
            registerderived!(ctx, rec, Tables.Schema)
            for (i, (col, ct)) in enumerate(zip(ts.names, ts.types))
                fn = String(get(names, col, string(col)))
                checkderivedname(fn, "column name \"$fn\"", "Tables.Schema column $col", "rename it with `names=Dict(:$col => \"…\")`")
                haskey(index, fn) && throw(ArgumentError("column name \"$fn\" derived twice"))
                push!(fields, Field(fn, derive(ctx, ct, nothing, full.namespace), nothing, nodefault, :ascending, freeze!(FrozenVector{String}()), Props()))
                index[fn] = i
            end
            freeze!(fields); freeze!(index)
            rec
        end
        finalizepublic!(s, limits, 0, 0)
    end
end

"""
    Avro.schema(x; limits=Limits()) -> Schema

The schema of a value: identity-bearing generic values (`Record`, `EnumValue`, `Fixed`) return their
instance schema; vectors/maps of identity-bearing values of one identity return the array/map of it;
other values use the conventional `Avro.schema(typeof(x))`. Mixed identities, `Vector{Any}`/`Map{Any}`,
empty collections of identity-bearing element types, a bare `UnionValue` and a `Decimal` raise
`ArgumentError` (use `Avro.encode(schema, x)`).
"""
function identityschema(s::Schema, limits::Limits)
    return withconstruction(limits; direction=:encode) do _
        finalizepublic!(s, limits, 0, 0)
        return s
    end
end

function schema(x::Record; limits::Limits=Limits())
    return identityschema(getfield(x, :schema), limits)
end

function schema(x::EnumValue; limits::Limits=Limits())
    return identityschema(x.schema, limits)
end

function schema(x::Fixed; limits::Limits=Limits())
    return identityschema(x.schema, limits)
end

function schema(::UnionValue; limits::Limits=Limits())
    throw(ArgumentError("a bare Avro.UnionValue has no schema of its own: use `Avro.encode(schema, x)`"))
end

function schema(::Union{Decimal,WideDecimal}; limits::Limits=Limits())
    throw(ArgumentError("a decimal needs a precision and scale: use `Avro.encode(schema, x)`"))
end

const IDENTITY_VALUES = Union{Record,EnumValue,Fixed}

function schema(x::AbstractVector{T}; limits::Limits=Limits()) where {T}
    T === UInt8 && return schema(Vector{UInt8}; limits=limits)
    (T <: IDENTITY_VALUES || T === Any) || return schema(typeof(x); limits=limits)
    return withconstruction(limits; direction=:encode) do _
        return ArraySchema(uniformschema(x, limits); limits=limits)
    end
end

function schema(x::Map{V}; limits::Limits=Limits()) where {V}
    (V <: IDENTITY_VALUES || V === Any) || return schema(typeof(x); limits=limits)
    return withconstruction(limits; direction=:encode) do _
        return MapSchema(uniformschema(x.vals, limits); limits=limits)
    end
end

function uniformschema(xs, limits::Limits)
    isempty(xs) && throw(ArgumentError("an empty collection of identity-bearing values has no schema of its own: use `Avro.encode(schema, x)`"))
    first = nothing
    for v in xs
        v isa IDENTITY_VALUES || throw(ArgumentError("a collection mixing identity-bearing and plain values has no schema of its own: use `Avro.encode(schema, x)`"))
        s = schema(v; limits=limits)
        if first === nothing
            first = s
        elseif fullname(s) != fullname(first)
            throw(ArgumentError("a collection of values with different schema identities has no schema of its own: use `Avro.encode(schema, x)`"))
        end
    end
    return first
end

function schema(x; limits::Limits=Limits())
    return schema(typeof(x); limits=limits)
end
