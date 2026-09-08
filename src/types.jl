# Julia type → schema derivation with the complete name policy (plan §4.8), and the value-level
# `Avro.schema(x)`.

using StructUtils: StructUtils

"""
    Avro.AvroStyle <: StructUtils.StructStyle

The StructUtils style used by typed decoding and by `Avro.schema(T)` to read field tags and defaults.
"""
struct AvroStyle <: StructUtils.StructStyle
    schemas::Union{Nothing,Vector{Schema}}
end

function AvroStyle()
    return AvroStyle(nothing)
end

function AvroStyle(schema::Schema)
    return AvroStyle(Schema[schema])
end

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

const DEFAULT_AVRONAME_METHOD = which(avroname, Tuple{Type{Int}})

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

"A fixed-prefix sink used by the internal, budgeted default `avroname` path."
mutable struct DerivedNameSink <: IO
    const bytes::Vector{UInt8}
    total::Int
end

function Base.write(sink::DerivedNameSink, byte::UInt8)
    sink.total = checked_add(sink.total, 1)
    sink.total <= length(sink.bytes) && (@inbounds sink.bytes[sink.total] = byte)
    return 1
end

function Base.unsafe_write(sink::DerivedNameSink, pointer::Ptr{UInt8}, count::UInt)
    n = Int(count)
    old = sink.total
    sink.total = checked_add(old, n)
    copied = min(n, max(length(sink.bytes) - old, 0))
    copied > 0 && GC.@preserve sink unsafe_copyto!(Base.pointer(sink.bytes, old + 1),
                                                   pointer, copied)
    return count
end

"One streamed parameter spelling, sanitised into a shared fixed-prefix name sink."
mutable struct DerivedParameterSink <: IO
    const output::DerivedNameSink
    emitted::Bool
    pending_separator::Bool
end

function reset!(sink::DerivedParameterSink)
    sink.emitted = false
    sink.pending_separator = false
    return sink
end

function Base.write(sink::DerivedParameterSink, byte::UInt8)
    valid = byte == UInt8('_') || UInt8('A') <= byte <= UInt8('Z') ||
            UInt8('a') <= byte <= UInt8('z') || UInt8('0') <= byte <= UInt8('9')
    if valid && byte != UInt8('_')
        if !sink.emitted && UInt8('0') <= byte <= UInt8('9')
            Base.write(sink.output, UInt8('_'))
        elseif sink.pending_separator && sink.emitted
            Base.write(sink.output, UInt8('_'))
        end
        Base.write(sink.output, byte)
        sink.emitted = true
        sink.pending_separator = false
    else
        sink.pending_separator = true
    end
    return 1
end

function Base.unsafe_write(sink::DerivedParameterSink, pointer::Ptr{UInt8}, count::UInt)
    for index in 1:Int(count)
        Base.write(sink, unsafe_load(pointer, index))
    end
    return count
end

function finishparameter!(sink::DerivedParameterSink)
    sink.emitted || Base.write(sink.output, UInt8('_'))
    return nothing
end

"A bounded-memory SHA-256 sink whose bytes are charged to the derivation budget."
mutable struct DerivedHashSink <: IO
    const context::SHA.SHA2_256_CTX
    const bytes::Vector{UInt8}
    len::Int
end

function flushhash!(sink::DerivedHashSink)
    sink.len == 0 && return nothing
    SHA.update!(sink.context, sink.bytes, sink.len)
    sink.len = 0
    return nothing
end

function Base.write(sink::DerivedHashSink, byte::UInt8)
    sink.len == length(sink.bytes) && flushhash!(sink)
    sink.len += 1
    @inbounds sink.bytes[sink.len] = byte
    return 1
end

function Base.unsafe_write(sink::DerivedHashSink, pointer::Ptr{UInt8}, count::UInt)
    remaining = Int(count)
    offset = 0
    while remaining > 0
        sink.len == length(sink.bytes) && flushhash!(sink)
        copied = min(remaining, length(sink.bytes) - sink.len)
        GC.@preserve sink unsafe_copyto!(Base.pointer(sink.bytes, sink.len + 1),
                                         pointer + offset, copied)
        sink.len += copied
        remaining -= copied
        offset += copied
    end
    return count
end

"Hash `string(T)` without materialising that potentially unbounded diagnostic spelling."
function derivedtypehash(budget::Budget, ::Type{T}) where {T}
    checkpoint = budgetcheckpoint(budget)
    bufferbytes = bytesbytes(4096)
    sinkbytes = shellbytes(DerivedHashSink)
    digestbytes = bytesbytes(32)
    buffer = digest = nothing
    try
        reserve!(budget, checked_add(bufferbytes, sinkbytes))
        buffer = Vector{UInt8}(undef, 4096)
        allocated!(budget, bufferbytes)
        sink = DerivedHashSink(SHA.SHA2_256_CTX(), buffer, 0)
        allocated!(budget, sinkbytes)
        print(sink, T)
        flushhash!(sink)
        reserve!(budget, digestbytes)
        digest = SHA.digest!(sink.context)
        allocated!(budget, digestbytes)
        buffer = nothing
        release!(budget, checked_add(bufferbytes, sinkbytes))
        return digest
    catch
        buffer = digest = nothing
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

function derivedtypehash(ctx, ::Type{T}) where {T}
    return derivedtypehash(ctx.budget, T)
end

"A charged type spelling with a fixed prefix and a streamed digest for long parameter lists."
function boundedtypedescription(budget::Budget, ::Type{T}) where {T}
    checkpoint = budgetcheckpoint(budget)
    prefixbytes = bytesbytes(129)
    sinkbytes = shellbytes(DerivedNameSink)
    prefix = digest = output = nothing
    try
        reserve!(budget, checked_add(prefixbytes, sinkbytes))
        prefix = Vector{UInt8}(undef, 129)
        allocated!(budget, prefixbytes)
        sink = DerivedNameSink(prefix, 0)
        allocated!(budget, sinkbytes)
        print(sink, T)
        if sink.total <= 128
            outlen = sink.total
            reserve!(budget, bytesbytes(outlen))
            output = Vector{UInt8}(undef, outlen)
            allocated!(budget, bytesbytes(outlen))
            copyto!(output, 1, prefix, 1, outlen)
        else
            stop = 100
            while stop > 0 && (prefix[stop + 1] & 0xc0) == 0x80
                stop -= 1
            end
            digest = derivedtypehash(budget, T)
            outlen = checked_add(stop, 17)
            reserve!(budget, bytesbytes(outlen))
            output = Vector{UInt8}(undef, outlen)
            allocated!(budget, bytesbytes(outlen))
            copyto!(output, 1, prefix, 1, stop)
            output[stop + 1] = UInt8('#')
            hex = codeunits("0123456789abcdef")
            for index in 1:8
                byte = digest[index]
                output[stop + 2 * index] = hex[Int(byte >> 4) + 1]
                output[stop + 2 * index + 1] = hex[Int(byte & 0x0f) + 1]
            end
            release!(budget, bytesbytes(length(digest)))
            digest = nothing
        end
        prefix = nothing
        release!(budget, checked_add(prefixbytes, sinkbytes))
        description = takeownedstring!(output, budget)
        output = nothing
        return description
    catch
        prefix = digest = output = nothing
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

"Build a bounded diagnostic containing one caller-defined type."
function typediagnostic(budget::Budget, prefix, type::Type, suffix...)
    description = boundedtypedescription(budget, type)
    try
        return diagnosticstring(budget, prefix, description, suffix...)
    finally
        release!(budget, stringbytes(sizeof(description)))
    end
end

"Build a bounded diagnostic containing two caller-defined types."
function typediagnostic(budget::Budget, prefix, first::Type, middle,
                        second::Type, suffix...)
    firstdescription = boundedtypedescription(budget, first)
    seconddescription = nothing
    try
        seconddescription = boundedtypedescription(budget, second)
        return diagnosticstring(budget, prefix, firstdescription, middle,
                                seconddescription, suffix...)
    finally
        release!(budget, stringbytes(sizeof(firstdescription)))
        seconddescription === nothing ||
            release!(budget, stringbytes(sizeof(seconddescription)))
    end
end

"Build the same bounded type diagnostic when conversion runs outside an operation budget."
function typediagnostic(::Nothing, parts...)
    return withbudget(Limits()) do budget
        return typediagnostic(budget, parts...)
    end
end

"The default derived type name with fixed prefix storage and a streamed long-name digest."
function defaultderivedname(ctx, ::Type{T}) where {T}
    checkpoint = budgetcheckpoint(ctx.budget)
    prefixbytes = bytesbytes(129)
    sinkbytes = checked_add(shellbytes(DerivedNameSink),
                            shellbytes(DerivedParameterSink))
    prefix = output = digest = nothing
    try
        reserve!(ctx.budget, checked_add(prefixbytes, sinkbytes))
        prefix = Vector{UInt8}(undef, 129)
        allocated!(ctx.budget, prefixbytes)
        sink = DerivedNameSink(prefix, 0)
        allocated!(ctx.budget, shellbytes(DerivedNameSink))
        parameter = DerivedParameterSink(sink, false, false)
        allocated!(ctx.budget, shellbytes(DerivedParameterSink))
        print(sink, nameof(T))
        params = T isa DataType ? T.parameters : ()
        if !isempty(params)
            Base.write(sink, UInt8('_'))
            for (index, value) in enumerate(params)
                index == 1 || Base.write(sink, UInt8('_'))
                reset!(parameter)
                print(parameter, value)
                finishparameter!(parameter)
            end
        end
        long = sink.total > 128
        outlen = long ? 117 : sink.total
        reserve!(ctx.budget, bytesbytes(outlen))
        output = Vector{UInt8}(undef, outlen)
        allocated!(ctx.budget, bytesbytes(outlen))
        if long
            copyto!(output, 1, prefix, 1, 100)
            output[101] = UInt8('_')
            digest = derivedtypehash(ctx, T)
            hex = codeunits("0123456789abcdef")
            for index in 1:8
                byte = digest[index]
                output[100 + 2 * index] = hex[Int(byte >> 4) + 1]
                output[101 + 2 * index] = hex[Int(byte & 0x0f) + 1]
            end
            release!(ctx.budget, bytesbytes(length(digest)))
            digest = nothing
        else
            copyto!(output, 1, prefix, 1, outlen)
        end
        name = takeownedstring!(output, ctx.budget)
        output = nothing
        prefix = nothing
        release!(ctx.budget, checked_add(prefixbytes, sinkbytes))
        return name
    catch
        prefix = output = digest = nothing
        rollbackreservations!(ctx.budget, checkpoint)
        rethrow()
    end
end

"The default module namespace, built directly under `max_name_bytes`."
function defaultderivednamespace(ctx, ::Type{T}) where {T}
    checkpoint = budgetcheckpoint(ctx.budget)
    writer = nothing
    try
        writer = BoundedWriter(ctx.budget, ctx.limits.max_name_bytes;
                               limit=:max_name_bytes, credit=false)
        for (index, component) in enumerate(Base.fullname(parentmodule(T)))
            index == 1 || Base.write(writer, UInt8('.'))
            print(writer, component)
        end
        namespace = boundedtake!(writer)
        release!(ctx.budget, bytesbytes(0))
        return namespace
    catch
        writer = nothing
        rollbackreservations!(ctx.budget, checkpoint)
        rethrow()
    end
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

const DEFAULT_AVROSYMBOL_METHOD = only(methods(avrosymbol))

struct DeriveContext
    limits::Limits
    budget::Budget
    named::FrozenDict{String,Schema}          # fullname → schema (first definition wins)
    origins::FrozenDict{String,Type}          # fullname → Julia type that defined it
    anonymous::Base.RefValue{Int}             # counter for nested NamedTuple records
end

"Create the two derivation memos under the shared construction budget."
function derivecontext(limits::Limits, budget::Budget, anonymous::Int)
    charge = 2 * frozendictshell()
    reserve!(budget, charge)
    ctx = DeriveContext(limits, budget, FrozenDict{String,Schema}(), FrozenDict{String,Type}(), Ref(anonymous))
    allocated!(budget, charge)
    return ctx
end

function releasederivecontext!(ctx::DeriveContext)
    keybytes = 0
    for name in ctx.named.keys
        keybytes = checked_add(keybytes, stringbytes(sizeof(name)))
    end
    release!(ctx.budget, frozendictbytes(ctx.named) + frozendictbytes(ctx.origins) + keybytes)
    return nothing
end

function nameerror(what, remedy)
    throw(ArgumentError("invalid Avro $what; $remedy"))
end

function checkderivedname(ctx::DeriveContext, name, what::String, remedy::String;
                          owned::Bool=false)
    name isa Union{AbstractString,Symbol} || nameerror(what, remedy)
    sizeof(name) <= ctx.limits.max_name_bytes ||
        throw(LimitError(:max_name_bytes, sizeof(name), ctx.limits.max_name_bytes,
                         :max_name_bytes, ctx.budget.direction))
    copy = owned ? name::String : ownedstringcopy(name, ctx.budget)
    isvalidname(copy) || nameerror(what, remedy)
    return copy
end

function checkderivednamespace(ctx::DeriveContext, ns; owned::Bool=false)
    ns isa AbstractString || nameerror("namespace", "override `Avro.avroname` for this type")
    sizeof(ns) <= ctx.limits.max_name_bytes ||
        throw(LimitError(:max_name_bytes, sizeof(ns), ctx.limits.max_name_bytes,
                         :max_name_bytes, ctx.budget.direction))
    isvalidnamespace(ns) || nameerror("namespace", "override `Avro.avroname` for this type")
    return owned ? ns::String : ownedstringcopy(ns, ctx.budget)
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
        ctx = derivecontext(limits, budget, 0)
        try
            s = withbuilder(() -> derive(ctx, T, name, namespace))
            return finalizepublic!(s, limits, 0, 0)
        finally
            releasederivecontext!(ctx)
        end
    end
end

const LOGICAL_LONG = Dict{Type,LogicalType}(
    Timestamp{Millisecond} => TimestampMillis(), Timestamp{Microsecond} => TimestampMicros(), Timestamp{Nanosecond} => TimestampNanos(),
    LocalTimestamp{Millisecond} => LocalTimestampMillis(), LocalTimestamp{Microsecond} => LocalTimestampMicros(),
    LocalTimestamp{Nanosecond} => LocalTimestampNanos(),
)

function derive(ctx::DeriveContext, ::Type{T}, name, namespace) where {T}
    existing = nothing
    for (full, origin) in ctx.origins
        origin === T || continue
        existing = ctx.named[full]
        break
    end
    existing === nothing || return existing
    nodebytes = NODE_META_BYTES + NODE_SHELL_BYTES
    reserve!(ctx.budget, nodebytes)                    # the derived node and its meta, settled once built
    s = try
        deriveimpl(ctx, T, name, namespace)
    catch
        unreserve!(ctx.budget, nodebytes)
        rethrow()
    end
    allocated!(ctx.budget, nodebytes)
    return s
end

function deriveimpl(ctx::DeriveContext, ::Type{T}, name, namespace) where {T}
    T === Missing && return NullSchema(EMPTY_PROPS, NodeMeta())
    T === Nothing && return NullSchema(EMPTY_PROPS, NodeMeta())
    T === Bool && return BooleanSchema(EMPTY_PROPS, NodeMeta())
    T in (Int8, Int16, Int32, UInt8, UInt16) && return IntSchema(nothing, EMPTY_PROPS, NodeMeta())
    T in (Int64, UInt32, UInt64) && return LongSchema(nothing, EMPTY_PROPS, NodeMeta())
    T in (Float16, Float32) && return FloatSchema(EMPTY_PROPS, NodeMeta())
    T === Float64 && return DoubleSchema(EMPTY_PROPS, NodeMeta())
    T === Vector{UInt8} && return BytesSchema(nothing, EMPTY_PROPS, NodeMeta())
    (T <: AbstractString || T === Symbol || T === Char) && return StringSchema(nothing, EMPTY_PROPS, NodeMeta())
    T === UUID && return StringSchema(UUIDLogical(), makeprops((;), ("type",), UUIDLogical()), NodeMeta())
    T === Date && return IntSchema(DateLogical(), makeprops((;), ("type",), DateLogical()), NodeMeta())
    T === Time && return LongSchema(TimeMicros(), makeprops((;), ("type",), TimeMicros()), NodeMeta())
    T === DateTime && return LongSchema(LocalTimestampMillis(), makeprops((;), ("type",), LocalTimestampMillis()), NodeMeta())
    haskey(LOGICAL_LONG, T) && return LongSchema(LOGICAL_LONG[T], makeprops((;), ("type",), LOGICAL_LONG[T]), NodeMeta())
    T <: DataDecimals.Decimal && return BytesSchema(; logical=DecimalLogical(precision(T), DataDecimals.scale(T)))
    T === Durations.Duration && return namedfixed(ctx, "Duration", namespace === nothing ? "" : namespace, 12, DurationLogical(), T)
    T === Duration && return namedfixed(ctx, "Duration", namespace === nothing ? "" : namespace, 12, DurationLogical(), T)
    (T <: DataDecimals.DecimalValue || T === WideDecimal) && throw(ArgumentError("a decimal needs a precision and scale: pass an explicit `schema=` (e.g. `Avro.BytesSchema(; logical=Avro.DecimalLogical(p, s))`)"))
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
    T <: Map && return MapSchema(derive(ctx, eltype(T).parameters[2], nothing, namespace), EMPTY_PROPS, NodeMeta())
    if T <: AbstractDict
        K, V = keytype(T), valtype(T)
        (K <: AbstractString || K === Symbol) ||
            throw(ArgumentError(typediagnostic(
                ctx.budget, "Avro maps have string keys; cannot derive a schema from ", T)))
        return MapSchema(derive(ctx, V, nothing, namespace), EMPTY_PROPS, NodeMeta())
    end
    T <: AbstractVector && return ArraySchema(derive(ctx, eltype(T), nothing, namespace), EMPTY_PROPS, NodeMeta())
    T <: NamedTuple && return derivenamedtuple(ctx, T, name, namespace)
    T <: Tuple && throw(ArgumentError("tuples other than NTuple{N,UInt8} have no conventional schema; use a NamedTuple or struct"))
    isstructtype(T) && isconcretetype(T) && return derivestruct(ctx, T, name, namespace)
    throw(ArgumentError(typediagnostic(
        ctx.budget, "no conventional Avro schema for ", T,
        "; pass an explicit `schema=`")))
end

function registerderived!(ctx::DeriveContext, s::NamedSchema, ::Type{T}) where {T}
    full = ownedfullname(s.name, ctx.budget)
    existing = budgetedget(ctx.named, full, nothing, ctx.budget)
    if existing !== nothing
        origin = budgetedgetindex(ctx.origins, full, ctx.budget)
        if origin === T
            release!(ctx.budget, stringbytes(sizeof(full)))
            return existing
        end
        throw(ArgumentError(typediagnostic(
            ctx.budget, "Julia types ", origin, " and ", T,
            " both derive the Avro fullname ", boundedquoted(full),
            "; override `Avro.avroname` for one of them")))
    end
    length(ctx.named) < ctx.limits.max_named_types || throw(LimitError(:max_named_types, length(ctx.named) + 1, ctx.limits.max_named_types, :max_named_types, :encode))
    budgetedinsert!(ctx.named, full, s, ctx.budget)
    budgetedinsert!(ctx.origins, full, T, ctx.budget)
    return s
end

function lookupderived(ctx::DeriveContext, full::FullName)
    name = ownedfullname(full, ctx.budget)
    try
        return budgetedget(ctx.named, name, nothing, ctx.budget)
    finally
        release!(ctx.budget, stringbytes(sizeof(name)))
    end
end

function namedfixed(ctx::DeriveContext, name::AbstractString, namespace::AbstractString,
                    size::Int, logical, ::Type{T}) where {T}
    namecopy = checkderivedname(ctx, name, "name", "pass an explicit schema")
    namespacecopy = checkderivednamespace(ctx, namespace)
    full = FullName(namecopy, namespacecopy)
    existing = lookupderived(ctx, full)
    if existing !== nothing
        release!(ctx.budget, stringbytes(sizeof(namecopy)) + stringbytes(sizeof(namespacecopy)))
        return registerderived!(ctx, existing, T)
    end
    p = makeprops((;), SCHEMA_GRAMMAR[:fixed], logical)
    s = FixedSchema(full, EMPTY_STRING_LIST, EMPTY_STRING_LIST, size, logical, p, NodeMeta())
    return registerderived!(ctx, s, T)
end

function derivedname(ctx::DeriveContext, ::Type{T}, name, namespace) where {T}
    custom = which(avroname, Tuple{Type{T}}) !== DEFAULT_AVRONAME_METHOD
    n = name
    ns = namespace
    nameowned = false
    namespaceowned = false
    if name === nothing || namespace === nothing
        if custom
            defaultname, defaultnamespace = avroname(T)
            name === nothing && (n = defaultname)
            namespace === nothing && (ns = defaultnamespace)
        else
            if name === nothing
                n = defaultderivedname(ctx, T)
                nameowned = true
            end
            if namespace === nothing
                ns = defaultderivednamespace(ctx, T)
                namespaceowned = true
            end
        end
    end
    remedy = "override `Avro.avroname` for this type or pass root `name=` and `namespace=`"
    n = checkderivedname(ctx, n, "name", remedy; owned=nameowned)
    ns = checkderivednamespace(ctx, ns; owned=namespaceowned)
    return FullName(n, ns)
end

function deriveenum(ctx::DeriveContext, ::Type{E}, name, namespace) where {E<:Base.Enum}
    full = derivedname(ctx, E, name, namespace)
    existing = lookupderived(ctx, full)
    if existing !== nothing
        release!(ctx.budget, stringbytes(sizeof(full.name)) + stringbytes(sizeof(full.namespace)))
        return registerderived!(ctx, existing, E)
    end
    n = length(instances(E))
    containers = vectorbytes(String, n) + 48 + vectorbytes(String, n) +
                 vectorbytes(Int, n)
    reserve!(ctx.budget, containers)
    syms = Vector{String}(undef, n)
    resize!(syms, 0)
    index = emptywithcapacity(FrozenDict{String,Int}, n)
    allocated!(ctx.budget, containers)
    customsymbols = which(avrosymbol, Tuple{Type{E},E}) !== DEFAULT_AVROSYMBOL_METHOD
    for (i, inst) in enumerate(instances(E))
        raw = customsymbols ? avrosymbol(E, inst) : Symbol(inst)
        sym = checkderivedname(ctx, raw, "enum symbol",
                               "override `Avro.avrosymbol` for this enum")
        budgetedhaskey(index, sym, ctx.budget) &&
            throw(ArgumentError(typediagnostic(
                ctx.budget, "enum ", E, " derives the symbol ",
                boundedquoted(sym), " twice")))
        push!(syms, sym)
        budgetedinsert!(index, sym, i, ctx.budget)
    end
    reserve!(ctx.budget, 24)
    symbols = freeze!(FrozenVector{String}(syms, false))
    allocated!(ctx.budget, 24)
    s = EnumSchema(full, EMPTY_STRING_LIST, EMPTY_STRING_LIST, nothing,
                   symbols, nodefault,
                   freeze!(index), EMPTY_PROPS, NodeMeta())
    return registerderived!(ctx, s, E)
end

function deriveunion(ctx::DeriveContext, ::Type{U}, namespace) where {U}
    members = Base.uniontypes(U)
    if length(members) == 2 && Missing in members && !(Nothing in members)
        other = members[1] === Missing ? members[2] : members[1]
        twobranch = frozenvectorshell() + 16
        reserve!(ctx.budget, twobranch)                # the two-branch vector, built next (§4.4)
        bs = FrozenVector{Schema}(Schema[derive(ctx, Missing, nothing, namespace),
                                         derive(ctx, other, nothing, namespace)], false)
        allocated!(ctx.budget, twobranch)
        return UnionSchema(freeze!(bs), NodeMeta())
    end
    branchbytes = 24 + vectorbytes(Schema, length(members))
    reserve!(ctx.budget, branchbytes)                  # shell and exact branch capacity (§4.4)
    bs = emptywithcapacity(FrozenVector{Schema}, length(members))
    allocated!(ctx.budget, branchbytes)
    reserve!(ctx.budget, frozendictshell())
    seen = FrozenDict{BranchIdentity,Int}()
    allocated!(ctx.budget, frozendictshell())
    try
        for (i, m) in enumerate(members)
            s = derive(ctx, m, nothing, namespace)
            s isa UnionSchema && throw(ArgumentError(typediagnostic(
                ctx.budget, "union member ", m,
                " derives a union; Avro unions cannot nest")))
            ident = branchidentity(s)
            previous = budgetedget(seen, ident, 0, ctx.budget)
            previous == 0 || throw(ArgumentError(typediagnostic(
                ctx.budget, "union members ", members[previous], " and ", m,
                " both map to the Avro branch ", boundedquoted(branchidentitylabel(s)),
                "; Avro unions cannot repeat a kind — use a named type or an explicit `schema=`")))
            budgetedinsert!(seen, ident, i, ctx.budget)
            push!(bs, s)
        end
    finally
        release!(ctx.budget, frozendictbytes(seen))
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
    full = FullName(checkderivedname(ctx, name, "name", "pass a valid `name=`"),
                    checkderivednamespace(ctx, ns))
    lookupderived(ctx, full) === nothing ||
        throw(ArgumentError("the Avro name \"$(fullname(full))\" is derived twice"))
    nf = length(names)
    slots = 24 + vectorbytes(Field, nf) + 48 + vectorbytes(String, nf) +
            vectorbytes(Int, nf)
    reserve!(ctx.budget, slots)                        # record shells and exact field capacity (§4.4)
    fields = emptywithcapacity(FrozenVector{Field}, nf)
    index = emptywithcapacity(FrozenDict{String,Int}, nf)
    allocated!(ctx.budget, slots)
    rec = RecordSchema(full, EMPTY_STRING_LIST, EMPTY_STRING_LIST, nothing, false,
                       EMPTY_PROPS, fields, index, NodeMeta())
    registerderived!(ctx, rec, T)
    for (i, (fname, ftype)) in enumerate(zip(names, types))
        fn = checkderivedname(ctx, fname, "field name", "rename the field")
        fs = derive(ctx, ftype, nothing, full.namespace)
        reserve!(ctx.budget, 128)
        field = Field(fn, fs, nothing, nodefault, :ascending, EMPTY_STRING_LIST,
                      EMPTY_PROPS)
        allocated!(ctx.budget, 128)
        budgetedpush!(fields, field, ctx.budget)
        budgetedinsert!(index, fn, i, ctx.budget)
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
    existing = lookupderived(ctx, full)
    existing === nothing || return registerderived!(ctx, existing, T)
    tags = StructUtils.fieldtags(AvroStyle(), T)
    defaults = StructUtils.fielddefaults(AvroStyle(), T)
    nf = fieldcount(T)
    slots = 24 + vectorbytes(Field, nf) + 48 + vectorbytes(String, nf) +
            vectorbytes(Int, nf)
    reserve!(ctx.budget, slots)                        # record shells and exact field capacity (§4.4)
    fields = emptywithcapacity(FrozenVector{Field}, nf)
    index = emptywithcapacity(FrozenDict{String,Int}, nf)
    allocated!(ctx.budget, slots)
    rec = RecordSchema(full, EMPTY_STRING_LIST, EMPTY_STRING_LIST, nothing, false,
                       EMPTY_PROPS, fields, index, NodeMeta())
    registerderived!(ctx, rec, T)
    for (i, fname) in enumerate(fieldnames(T))
        ftype = fieldtype(T, i)
        tagged = fieldtag(tags, fname, :name)
        rawname = tagged === nothing ? fname : tagged
        fn = checkderivedname(ctx, rawname, "field name",
                              "tag the field with `&(avro=(name=\"…\",),)`")
        budgetedhaskey(index, fn, ctx.budget) &&
            throw(ArgumentError(typediagnostic(
                ctx.budget, "struct ", T, " derives the field name ",
                boundedquoted(fn), " twice")))
        fs = derive(ctx, ftype, nothing, full.namespace)
        d = nodefault
        tagdefault = fieldtag(tags, fname, :default)
        if tagdefault !== nothing
            j = tojsonvalue(tagdefault, ctx.budget)
            ok, branch = validatedefault(fs, j, ctx.limits.max_depth, 1,
                                         ctx.budget)
            ok || throw(ArgumentError(typediagnostic(
                ctx.budget, "the `avro=(default=…,)` tag of ", T, ".",
                boundedquoted(fname), " is not a valid default for its schema")))
            jw = BoundedWriter(ctx.budget, ctx.limits.max_schema_bytes)
            printjson(jw, j, false, 0)
            d = DefaultValue(j, branch, boundedtake!(jw), 0, true)
        elseif haskey(defaults, fname)
            dv = defaults[fname]
            j = try
                tojsonvalue(dv === missing ? nothing : dv, ctx.budget)
            catch err
                err isa UnsupportedJSONValueError || rethrow()
                throw(ArgumentError(typediagnostic(
                    ctx.budget, "the default of ", T, ".", boundedquoted(fname),
                    " is not JSON-encodable under its schema; supply one with the `&(avro=(default=…,),)` tag")))
            end
            ok, branch = validatedefault(fs, j, ctx.limits.max_depth, 1,
                                         ctx.budget)
            ok || throw(ArgumentError(typediagnostic(
                ctx.budget, "the default of ", T, ".", boundedquoted(fname),
                " is not JSON-encodable under its schema; supply one with the `&(avro=(default=…,),)` tag")))
            jw = BoundedWriter(ctx.budget, ctx.limits.max_schema_bytes)
            printjson(jw, j, false, 0)
            d = DefaultValue(j, branch, boundedtake!(jw), 0, true)
        end
        reserve!(ctx.budget, 128)
        field = Field(fn, fs, nothing, d, :ascending, EMPTY_STRING_LIST,
                      EMPTY_PROPS)
        allocated!(ctx.budget, 128)
        budgetedpush!(fields, field, ctx.budget)
        budgetedinsert!(index, fn, i, ctx.budget)
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
        ctx = derivecontext(limits, budget, 1)
        try
            s = withbuilder() do
                full = FullName(checkderivedname(ctx, name, "name", "pass a valid `name=`"),
                                checkderivednamespace(ctx, namespace))
                nf = length(ts.names)
                slots = 24 + vectorbytes(Field, nf) + 48 + vectorbytes(String, nf) +
                        vectorbytes(Int, nf)
                reserve!(budget, slots)
                fields = emptywithcapacity(FrozenVector{Field}, nf)
                index = emptywithcapacity(FrozenDict{String,Int}, nf)
                allocated!(budget, slots)
                rec = publicnode(RecordSchema(full, EMPTY_STRING_LIST, EMPTY_STRING_LIST,
                                              nothing, false, EMPTY_PROPS, fields, index,
                                              publicmeta()))
                registerderived!(ctx, rec, Tables.Schema)
                for (i, (col, ct)) in enumerate(zip(ts.names, ts.types))
                    renamed = get(names, col, nothing)
                    rawname = renamed === nothing ? col : renamed
                    fn = checkderivedname(ctx, rawname, "column name",
                                          "rename it with `names=Dict(:$col => \"…\")`")
                    budgetedhaskey(index, fn, budget) &&
                        throw(ArgumentError("column name \"$fn\" derived twice"))
                    fs = derive(ctx, ct, nothing, full.namespace)
                    reserve!(budget, 128)
                    field = Field(fn, fs, nothing, nodefault, :ascending,
                                  EMPTY_STRING_LIST, EMPTY_PROPS)
                    allocated!(budget, 128)
                    budgetedpush!(fields, field, budget)
                    budgetedinsert!(index, fn, i, budget)
                end
                freeze!(fields)
                freeze!(index)
                return rec
            end
            return finalizepublic!(s, limits, 0, 0)
        finally
            releasederivecontext!(ctx)
        end
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
    return withconstruction(limits; direction=:encode) do budget
        return checkexistinggraph!(s, limits, budget)
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
        elseif !fullnameequal(s.name, first.name)
            throw(ArgumentError("a collection of values with different schema identities has no schema of its own: use `Avro.encode(schema, x)`"))
        end
    end
    return first
end

function schema(x; limits::Limits=Limits())
    return schema(typeof(x); limits=limits)
end
