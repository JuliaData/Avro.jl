# Write plans: one node per schema node. The generic route encodes values by their run-time types: generic
# values, NamedTuples, StructUtils structs, Tables rows, dictionaries and iterables; each value is checked
# against its schema node, and a union's branch is chosen from the value. Typed write nodes (typedwrite.jl)
# encode through the same per-plan `encodevalue` methods.

struct WArray <: Plan
    schema::ArraySchema
    items::Plan
end
struct WMap <: Plan
    schema::MapSchema
    values::Plan
end

# `nullable` is the position of the null branch of a two-branch nullable union (0 otherwise); `reprtypes`
# holds each branch's `juliatype` for exact matches.
struct WUnion <: Plan
    schema::UnionSchema
    branches::Vector{Plan}
    nullable::Int
    reprtypes::Vector{Any}
end

# How one Julia record type maps onto a record plan: the field of `type` for each schema field, and whether
# the type overrides `propertynames` (a DataFrame row, say), whose properties rather than its fields are the
# record's.
struct RecordLayout
    type::Any
    positions::Vector{Int}
    properties::Bool
end

mutable struct WRecord <: Plan
    const schema::RecordSchema
    const fields::Vector{Plan}
    @atomic layout::Union{Nothing,RecordLayout}   # the layout of the last record type encoded
end

function WRecord(schema::RecordSchema, fields::Vector{Plan})
    return WRecord(schema, fields, nothing)
end

# One plan node per schema node, memoised by node id; a record is memoised before its fields are built
# so recursive schemas terminate.
function writeplan(@nospecialize(s::Schema))
    return writeplan(s, Vector{Union{Nothing,Plan}}(nothing, graphinfo(s).nodes))
end

function writeplan(@nospecialize(s::Schema), memo::Vector{Union{Nothing,Plan}})
    id = Int(nodeid(s)) + 1
    p = memo[id]
    p === nothing || return p
    p = buildwriteplan(s, memo)
    memo[id] = p
    return p
end

# The composites have write plans of their own; the leaves are shared with reading.
function buildwriteplan(@nospecialize(s::Schema), memo::Vector{Union{Nothing,Plan}})
    s isa ArraySchema && return WArray(s, writeplan(s.items, memo))
    s isa MapSchema && return WMap(s, writeplan(s.values, memo))
    if s isa UnionSchema
        branches = Plan[]
        reprtypes = Any[]
        for b in s.branches
            push!(branches, writeplan(b, memo))
            push!(reprtypes, juliatype(b))
        end
        return WUnion(s, branches, nullablebranch(s), reprtypes)
    end
    s isa RecordSchema || return buildreadplan(s, memo, :standard)
    p = WRecord(s, Plan[])
    memo[Int(nodeid(s)) + 1] = p
    for f in s.fields
        push!(p.fields, writeplan(f.schema, memo))
    end
    return p
end

# ---- value paths and errors -----------------------------------------------------------------------------

# Out of line and without constant propagation, so that a message built from constants stays a call
# `juliac --trim` resolves.
@noinline Base.@constprop :none function encodeerror(msg::AbstractString, x)
    throw(EncodeError(string(msg, " (got ", nameof(typeof(x)), ")"), "", nothing))
end

# Encode `x` under `p` as one counted value. Frames below the root leave the value path and expected
# schema in place when something throws: the root frame reads them for the error, then restores them.
function encode(p::Plan, e::Encoder, x)
    countvalue!(e)
    encodevalue(p, e, x)
    return nothing
end

# One statically typed value with its path frame.
@inline function encodeframed(plan::P, e::Encoder, value::T, schema::Schema, kind::UInt8,
                              name::Union{Nothing,AbstractString,Symbol}) where {P<:Plan,T}
    previous = e.expected
    pushencodepath!(e, kind, name, 0)
    e.expected = schema
    encode(plan, e, value)
    e.expected = previous
    popencodepath!(e)
    return nothing
end

# One root datum: the value path starts empty, `schema` is the expected schema, and an `EncodeError` raised
# below leaves with the path and schema of the value that raised it.
function encodedatum!(plan::Plan, e::Encoder, x, schema::Schema)
    previous = e.expected
    depth = e.depth
    empty!(e.path)
    e.expected = schema
    try
        encode(plan, e, x)
    catch err
        err isa EncodeError && isempty(err.path) && throw(EncodeError(err.msg, formatencodepath(e), e.expected))
        rethrow()
    finally
        e.expected = previous
        e.depth = depth
        empty!(e.path)
    end
    return nothing
end

# ---- scalars ---------------------------------------------------------------------------------------------

# Inlined, so that a typed leaf's chain over the plan types raises its error without a method per plan.
@inline function encodevalue(p::Plan, e::Encoder, x)
    return encodeerror(expected(p), x)
end

# What a leaf plan accepts, for the error on any other value.
function expected(@nospecialize(p::Plan))
    p isa NullPlan && return "expected null (missing or nothing)"
    p isa BoolPlan && return "expected a Bool"
    p isa Union{IntPlan,LongPlan} && return "expected an integer"
    p isa FloatPlan && return "expected a Float32 (a Float64 is not accepted for an Avro float)"
    p isa DoublePlan && return "expected a Float64"
    p isa BytesPlan && return "expected bytes (an AbstractVector{UInt8})"
    p isa StringPlan && return "expected a string"
    p isa FixedPlan && return expectedfixed(p.schema.size)
    p isa EnumPlan && return expectedenum(fullname(p.schema))
    p isa DatePlan && return "expected a Date"
    p isa Union{TimeMillisPlan,TimeMicrosPlan} && return "expected a Time"
    p isa TimestampPlan{Millisecond} && return "expected an instant (Avro.Timestamp{Millisecond}, any Durations.ZonedTimestamp) or a DateTime"
    p isa TimestampPlan{Microsecond} && return "expected an instant (Avro.Timestamp{Microsecond}, any Durations.ZonedTimestamp) or a DateTime"
    p isa TimestampPlan{Nanosecond} && return "expected an instant (Avro.Timestamp{Nanosecond}, any Durations.ZonedTimestamp) or a DateTime"
    p isa LocalTimestampPlan{Millisecond} && return "expected a local time (Avro.LocalTimestamp{Millisecond}) or a DateTime"
    p isa LocalTimestampPlan{Microsecond} && return "expected a local time (Avro.LocalTimestamp{Microsecond}) or a DateTime"
    p isa LocalTimestampPlan{Nanosecond} && return "expected a local time (Avro.LocalTimestamp{Nanosecond}) or a DateTime"
    p isa DecimalPlan && return expecteddecimal(p.scale)
    p isa Union{UUIDStringPlan,UUIDFixedPlan} && return "expected a UUID"
    p isa DurationPlan && return "expected an Avro.Duration"
    return "unexpected value"
end

@noinline Base.@constprop :none function expectedfixed(size::Int)
    return string("expected ", size, " fixed bytes")
end

@noinline Base.@constprop :none function expectedenum(name::String)
    return string("expected an enum symbol of ", name)
end

@noinline Base.@constprop :none function expecteddecimal(scale::Int)
    return string("expected an Avro.Decimal/WideDecimal with scale ", scale)
end

function encodevalue(::NullPlan, e::Encoder, x::Union{Missing,Nothing})
    return nothing
end

function encodevalue(::BoolPlan, e::Encoder, x::Bool)
    return writebool!(e, x)
end

function encodevalue(p::IntPlan, e::Encoder, x::Integer)
    x isa Bool && encodeerror(expected(p), x)
    typemin(Int32) <= x <= typemax(Int32) || encodeerror("integer does not fit an Avro int", x)
    writeint!(e, Int32(x))
    return nothing
end

function encodevalue(p::LongPlan, e::Encoder, x::Integer)
    x isa Bool && encodeerror(expected(p), x)
    typemin(Int64) <= x <= typemax(Int64) || encodeerror("integer does not fit an Avro long", x)
    writelong!(e, Int64(x))
    return nothing
end

function encodevalue(::FloatPlan, e::Encoder, x::Union{Float32,Float16})
    return writefloat!(e, Float32(x))
end

function encodevalue(::FloatPlan, e::Encoder, x::Integer)
    x isa Bool && encodeerror("expected a Float32", x)
    y = Float32(x)
    y == x || encodeerror("the integer is not exactly representable as an Avro float", x)
    return writefloat!(e, y)
end

function encodevalue(::DoublePlan, e::Encoder, x::Union{Float64,Float32,Float16})
    return writedouble!(e, Float64(x))
end

function encodevalue(::DoublePlan, e::Encoder, x::AbstractFloat)
    y = Float64(x)
    (y == x || isnan(x)) || encodeerror("the value is not exactly representable as an Avro double", x)
    return writedouble!(e, y)
end

function encodevalue(::DoublePlan, e::Encoder, x::Integer)
    x isa Bool && encodeerror("expected a Float64", x)
    y = Float64(x)
    y == x || encodeerror("the integer is not exactly representable as an Avro double", x)
    return writedouble!(e, y)
end

function encodevalue(::BytesPlan, e::Encoder, x::AbstractVector{UInt8})
    return writebytes!(e, x)
end

function encodevalue(::StringPlan, e::Encoder, x::AbstractString)
    isstrictutf8(x) || encodeerror("string is not valid UTF-8", x)
    writestring!(e, x)
    return nothing
end

function encodevalue(p::StringPlan, e::Encoder, x::Symbol)
    return encodevalue(p, e, String(x))
end

function encodevalue(::StringPlan, e::Encoder, x::Char)
    isvalid(x) || encodeerror("string is not valid UTF-8", x)
    return writechar!(e, x)
end

function encodevalue(p::FixedPlan, e::Encoder, x::Fixed)
    (x.schema.name == p.schema.name && x.schema.size == p.schema.size) || encodeerror("fixed value does not match the writer schema", x)
    length(x.bytes) == p.schema.size || encodeerror("fixed $(fullname(p.schema)) needs exactly $(p.schema.size) bytes, got $(length(x.bytes))", x)
    return writeraw!(e, x.bytes)
end

function encodevalue(p::FixedPlan, e::Encoder, x::AbstractVector{UInt8})
    length(x) == p.schema.size || encodeerror("fixed $(fullname(p.schema)) needs exactly $(p.schema.size) bytes, got $(length(x))", x)
    return writeraw!(e, x)
end

function encodevalue(p::FixedPlan, e::Encoder, x::NTuple{N,UInt8}) where {N}
    N == p.schema.size || encodeerror("fixed $(fullname(p.schema)) needs exactly $(p.schema.size) bytes, got $N", x)
    ensureroom!(e, N)
    for b in x
        writebyte!(e, b)
    end
    return nothing
end

function encodevalue(p::EnumPlan, e::Encoder, x::EnumValue)
    if x.schema === p.schema || (x.schema.name == p.schema.name && x.schema.symbols == p.schema.symbols)
        writelong!(e, Int64(x.index) - 1)
        return nothing
    end
    return encodesymbol(p, e, String(x))
end

function encodevalue(p::EnumPlan, e::Encoder, x::AbstractString)
    return encodesymbol(p, e, x)
end

function encodevalue(p::EnumPlan, e::Encoder, x::Symbol)
    return encodesymbol(p, e, String(x))
end

function encodevalue(p::EnumPlan, e::Encoder, x::Base.Enum)
    return encodesymbol(p, e, avrosymbol(typeof(x), x))
end

function encodesymbol(p::EnumPlan, e::Encoder, symbol::AbstractString)
    i = get(p.schema.symbolindex, symbol, 0)
    i == 0 && encodeerror(string("the value is not a symbol of enum ", fullname(p.schema)), symbol)
    return writelong!(e, Int64(i) - 1)
end

function encodevalue(::DatePlan, e::Encoder, x::Date)
    days = Dates.value(x - DATE_EPOCH)
    typemin(Int32) <= days <= typemax(Int32) ||
        return encodeerror("date is outside the Avro int range", x)
    return writeint!(e, Int32(days))
end

function encodevalue(::TimeMillisPlan, e::Encoder, x::Time)
    ns = Dates.value(x)
    ns % 1_000_000 == 0 || encodeerror("time-millis needs a millisecond-aligned Time (use Avro.truncate/Avro.round)", x)
    writeint!(e, Int32(ns ÷ 1_000_000))
    return nothing
end

function encodevalue(::TimeMicrosPlan, e::Encoder, x::Time)
    ns = Dates.value(x)
    ns % 1_000 == 0 || encodeerror("time-micros needs a microsecond-aligned Time (use Avro.truncate/Avro.round)", x)
    writelong!(e, ns ÷ 1_000)
    return nothing
end

function encodevalue(::TimestampPlan{P}, e::Encoder, x::Timestamp{P}) where {P}
    return writelong!(e, Dates.value(x))
end

function encodevalue(::TimestampPlan{P}, e::Encoder, x::Durations.ZonedTimestamp) where {P}
    return writelong!(e, exactticks(P, x.utc, x))
end

function encodevalue(::TimestampPlan{P}, e::Encoder, x::DateTime) where {P}
    return writelong!(e, datetimeticks(P, x))
end

function encodevalue(::LocalTimestampPlan{P}, e::Encoder, x::LocalTimestamp{P}) where {P}
    return writelong!(e, Dates.value(x))
end

function encodevalue(::LocalTimestampPlan{P}, e::Encoder, x::LocalTimestamp) where {P}
    return writelong!(e, exactticks(P, x, x))
end

function encodevalue(::LocalTimestampPlan{P}, e::Encoder, x::DateTime) where {P}
    return writelong!(e, datetimeticks(P, x))
end

# The ticks of `ts` in unit `P`; a value that unit cannot represent exactly is refused, never rounded.
function exactticks(::Type{P}, ts::LocalTimestamp, x) where {P}
    try
        return Dates.value(convert(LocalTimestamp{P}, ts))
    catch err
        err isa InexactError || rethrow()
        encodeerror("the timestamp is not exactly representable in $(nameof(P))s", x)
    end
end

function encodevalue(p::DecimalPlan, e::Encoder, x::Union{Decimal,WideDecimal})
    x.scale == p.scale || encodeerror("decimal scale $(x.scale) does not equal the schema scale $(p.scale) (rescale first)", x)
    ndigits(x.unscaled) <= p.precision || encodeerror("decimal exceeds precision $(p.precision)", x)
    return writetwoscomplement!(e, p, x.unscaled)
end

function encodevalue(p::DecimalPlan, e::Encoder, x::DataDecimals.AbstractDecimal)
    return encodevalue(p, e, decimalinput(x))
end

# The byte length of `v`'s minimal big-endian two's complement form.
function twoscomplementlength(v::Int128)
    bits = reinterpret(UInt128, v)
    n = 16
    while n > 1
        lead = UInt8((bits >> (8 * (n - 1))) & 0xff)
        next = UInt8((bits >> (8 * (n - 2))) & 0xff)
        ((lead == 0x00 && next < 0x80) || (lead == 0xff && next >= 0x80)) || break
        n -= 1
    end
    return n
end

function filltwoscomplement!(out::Vector{UInt8}, v::Int128)
    bits = reinterpret(UInt128, v)
    for i in 0:length(out) - 1
        @inbounds out[end - i] = UInt8((bits >> (8 * i)) & 0xff)
    end
    return out
end

# The minimal big-endian two's complement bytes of `v`, as a new vector.
function twoscomplement(v::Int128)
    return filltwoscomplement!(Vector{UInt8}(undef, twoscomplementlength(v)), v)
end

function twoscomplement(v::BigInt)
    n = cld(ndigits(v < 0 ? -v - 1 : v; base=2) + 1, 8)
    return hex2bytes(string(v < 0 ? v + (BigInt(1) << (8 * n)) : v; base=16, pad=2 * n))
end

function writetwoscomplement!(e::Encoder, p::DecimalPlan, v::Union{Int128,BigInt})
    bytes = twoscomplement(v)
    fixed = p.fixedsize
    fixed == 0 && return writebytes!(e, bytes)
    length(bytes) <= fixed || encodeerror("decimal does not fit a fixed of $fixed bytes", v)
    pad = v < 0 ? 0xff : 0x00
    ensureroom!(e, fixed)
    for _ in 1:fixed - length(bytes)
        writebyte!(e, pad)
    end
    return writeraw!(e, bytes)
end

# The canonical lowercase UUID text, written directly into the encoder.
function writeuuidstring!(e::Encoder, u::UUID)
    writelong!(e, Int64(36))
    ensureroom!(e, 36)
    value = UInt128(u)
    pos = e.pos
    @inbounds for i in 0:31
        if i == 8 || i == 12 || i == 16 || i == 20
            pos += 1
            e.buf[pos] = UInt8('-')
        end
        nibble = UInt8((value >> (4 * (31 - i))) & 0x0f)
        pos += 1
        e.buf[pos] = nibble < 0x0a ? UInt8('0') + nibble : UInt8('a') - 0x0a + nibble
    end
    e.pos = pos
    return nothing
end

# The 16 big-endian UUID bytes, written directly into the encoder.
function writeuuidfixed!(e::Encoder, u::UUID)
    ensureroom!(e, 16)
    value = UInt128(u)
    pos = e.pos
    @inbounds for i in 0:15
        e.buf[pos + i + 1] = UInt8((value >> (8 * (15 - i))) & 0xff)
    end
    e.pos = pos + 16
    return nothing
end

function encodevalue(::UUIDStringPlan, e::Encoder, x::UUID)
    return writeuuidstring!(e, x)
end

function encodevalue(::UUIDStringPlan, e::Encoder, x::AbstractString)
    tryparse(UUID, x) === nothing && encodeerror("not an RFC 4122 uuid string", x)
    writestring!(e, x)
    return nothing
end

function encodevalue(::UUIDFixedPlan, e::Encoder, x::UUID)
    return writeuuidfixed!(e, x)
end

function encodevalue(::DurationPlan, e::Encoder, x::Duration)
    ensureroom!(e, 12)
    for v in (x.months, x.days, x.millis)
        for i in 0:3
            writebyte!(e, UInt8((v >> (8 * i)) & 0xff))
        end
    end
    return nothing
end

function encodevalue(p::DurationPlan, e::Encoder, x::Durations.Duration)
    0 <= x.months && 0 <= x.days && 0 <= x.nanoseconds ||
        encodeerror("Avro duration components must be nonnegative", x)
    q, r = divrem(x.nanoseconds, 1_000_000)
    iszero(r) && q <= typemax(UInt32) ||
        encodeerror("Avro duration requires UInt32 milliseconds without rounding", x)
    return encodevalue(p, e, Duration(x))
end

# ---- arrays and maps -------------------------------------------------------------------------------------

# A sized input is written as one block; an input of unknown length as one-item blocks.
function encodevalue(p::WArray, e::Encoder, x)
    items = arrayitems(x)
    enter!(e)
    previous = e.expected
    pushencodepath!(e, ENCODE_PATH_INDEX, nothing, 0)
    e.expected = p.schema.items
    encodeitems!(p.items, e, items, p.schema)
    e.expected = previous
    popencodepath!(e)
    writelong!(e, Int64(0))
    leave!(e)
    return nothing
end

# A function barrier on the concrete item plan and the caller's collection, so items encode without
# dynamic dispatch. Sized collections write one block; others write one block per item.
function encodeitems!(plan::Plan, e::Encoder, items, schema::ArraySchema)
    if Base.IteratorSize(typeof(items)) isa Union{Base.HasLength,Base.HasShape}
        n = length(items)
        n > 0 && writelong!(e, Int64(n))
        actual = 0
        for v in items
            actual += 1
            actual <= n || arraysizeerror(e, schema, "array iterator produced more than its declared length $n", items)
            setitempath!(e, actual - 1)
            encode(plan, e, v)
        end
        actual == n || arraysizeerror(e, schema, "array iterator produced $actual values but declared length $n", items)
    else
        index = 0
        for v in items
            writelong!(e, Int64(1))
            setitempath!(e, index)
            encode(plan, e, v)
            index += 1
        end
    end
    return nothing
end

@inline function setitempath!(e::Encoder, index::Int)
    @inbounds e.path[end] = EncodePathSegment(ENCODE_PATH_INDEX, nothing, index)
    return nothing
end

# A collection whose length lied is reported at the collection, not at its last item.
@noinline function arraysizeerror(e::Encoder, schema::ArraySchema, msg::AbstractString, items)
    popencodepath!(e)
    e.expected = schema
    encodeerror(msg, items)
end

function arrayitems(x::AbstractVector)
    return x
end

function arrayitems(x::Tuple)
    return x
end

function arrayitems(x::AbstractSet)
    return x
end

function arrayitems(x::AbstractString)
    return encodeerror("expected an array, not a string", x)
end

function arrayitems(x::AbstractDict)
    return encodeerror("expected an array, not a dictionary", x)
end

function arrayitems(x)
    !(x isa NOT_ARRAYLIKE) && applicable(iterate, x) && return x
    return encodeerror("expected an array (an iterable collection)", x)
end

function encodevalue(p::WMap, e::Encoder, x)
    entries = mappairs(x)
    enter!(e)
    n = length(entries)
    if n > 0
        writelong!(e, Int64(n))
        encodeentries!(p.values, e, entries, n, p.schema.values)
    end
    writelong!(e, Int64(0))
    leave!(e)
    return nothing
end

# A function barrier like `encodeitems!`.
function encodeentries!(plan::Plan, e::Encoder, entries, n::Int, schema::Schema)
    seen = distinctkeys(entries) ? nothing : Set{String}()
    i = 0
    for (k, v) in entries
        i += 1
        i <= n || encodeerror("map size changed during iteration", entries)
        key = mapkeystring(k)
        if seen !== nothing
            key in seen && encodeerror("map keys are duplicates after conversion to strings", entries)
            push!(seen, key)
        end
        writestring!(e, key)
        encodeframed(plan, e, v, schema, ENCODE_PATH_KEY, key)
    end
    i == n || encodeerror("map size changed during iteration", entries)
    return nothing
end

# A `Dict` keyed by one string-like type and a NamedTuple cannot hold two keys with one spelling; other
# dictionaries (mixed key types, `Base.ImmutableDict`) are checked for duplicates after conversion.
function distinctkeys(entries)
    return entries isa Union{Dict{String},Dict{Symbol},Base.Pairs{Symbol,<:Any,<:Any,<:NamedTuple}}
end

function mappairs(x::AbstractDict)
    return (keytype(x) <: Union{AbstractString,Symbol} || keytype(x) === Any) ? x : encodeerror("map keys must be strings or symbols", x)
end

function mappairs(x::NamedTuple)
    return pairs(x)
end

function mappairs(x)
    return encodeerror("expected a map (an AbstractDict or NamedTuple)", x)
end

function mapkeystring(k::Union{AbstractString,Symbol})
    key = String(k)::String
    isstrictutf8(key) || encodeerror("map key is not valid UTF-8", k)
    return key
end

function mapkeystring(k)
    return encodeerror("map keys must be strings or symbols", k)
end

# ---- unions ----------------------------------------------------------------------------------------------

function encodevalue(p::WUnion, e::Encoder, x)
    i = selectbranch(p, x)
    writelong!(e, Int64(i) - 1)
    value = x isa UnionValue ? x.value : x
    previous = e.expected
    e.expected = p.schema.branches[i]
    encode(p.branches[i], e, value)
    e.expected = previous
    return nothing
end

function encodevalue(p::WUnion, e::Encoder, x::Base.Enum)
    i, symbol = selectenumbranch(p, x)
    writelong!(e, Int64(i) - 1)
    countvalue!(e)
    encodesymbol(p.branches[i]::EnumPlan, e, symbol)
    return nothing
end

# The 1-based branch `x` is written under: a `UnionValue`'s own choice, the nullable split, a generic
# value's named identity, an exact `juliatype` match, a `Base.Enum`'s symbol, or the first accepting
# branch (records by field coverage).
function selectbranch(p::WUnion, x)
    n = length(p.branches)
    n == 0 && encodeerror("an empty union accepts no value", x)
    if x isa UnionValue
        1 <= x.index <= n || encodeerror("union branch index $(x.index) out of range (1:$n)", x)
        return x.index
    end
    if p.nullable != 0
        (x === missing || x === nothing) && return p.nullable
        return 3 - p.nullable
    end
    if x isa Union{Record,EnumValue,Fixed}
        s = x isa Record ? getfield(x, :schema) : x.schema
        for (i, b) in enumerate(p.schema.branches)
            typeof(b) === typeof(s) || continue
            b.name == s.name || continue
            b isa FixedSchema && s isa FixedSchema && b.size != s.size && continue
            return i
        end
    end
    T = typeof(x)
    for (i, r) in enumerate(p.reprtypes)
        r === T && accepts(p.branches[i], x) && return i
    end
    x isa Base.Enum && return first(selectenumbranch(p, x))
    for (i, b) in enumerate(p.branches)
        accepts(b, x) || continue
        return b isa WRecord ? recordbranch(p, i, x) : i
    end
    encodeerror("no union branch accepts the value", x)
end

# Of the record branches accepting `x`, the first that `x` fills completely with the most fields.
function recordbranch(p::WUnion, first::Int, x)
    best, most = first, recordcover(p.branches[first]::WRecord, x)
    for i in first + 1:length(p.branches)
        b = p.branches[i]
        b isa WRecord && accepts(b, x) || continue
        n = recordcover(b, x)
        n > most && ((best, most) = (i, n))
    end
    return best
end

# The number of `p`'s fields that `x` supplies, or -1 when `x` lacks one of them.
function recordcover(p::WRecord, x)
    if x isa AbstractDict
        values = recorddictvalues(p, x)
        return any(v -> v === DICT_MISSING, values) ? -1 : length(values)
    end
    positions = x isa TableRow ? recordrowpositions(p, x.row) :
                x isa Tables.AbstractRow || recordlayout(p, typeof(x)).properties ? recordrowpositions(p, x) :
                recordlayout(p, typeof(x)).positions
    return any(iszero, positions) ? -1 : length(positions)
end

function selectenumbranch(p::WUnion, x::Base.Enum)
    symbol = avrosymbol(typeof(x), x)
    symbol isa AbstractString || encodeerror("Avro.avrosymbol must return a string", x)
    for (i, branch) in enumerate(p.branches)
        branch isa EnumPlan || continue
        haskey(branch.schema.symbolindex, symbol) && return (i, symbol)
    end
    encodeerror("no union enum branch accepts the symbol", x)
end

function accepts(::NullPlan, x)
    return x === missing || x === nothing
end

function accepts(::BoolPlan, x)
    return x isa Bool
end

function accepts(::IntPlan, x)
    return x isa Integer && !(x isa Bool) && typemin(Int32) <= x <= typemax(Int32)
end

function accepts(::LongPlan, x)
    return x isa Integer && !(x isa Bool) && typemin(Int64) <= x <= typemax(Int64)
end

function accepts(::FloatPlan, x)
    return x isa Union{Float32,Float16} || (x isa Integer && !(x isa Bool) && Float32(x) == x)
end

function accepts(::DoublePlan, x)
    return x isa AbstractFloat || (x isa Integer && !(x isa Bool) && Float64(x) == x)
end

function accepts(::BytesPlan, x)
    return x isa AbstractVector{UInt8}
end

function accepts(::StringPlan, x)
    return x isa AbstractString || x isa Symbol || x isa Char
end

function accepts(p::FixedPlan, x)
    return (x isa Fixed && x.schema.name == p.schema.name) || ((x isa AbstractVector{UInt8} || x isa NTuple{N,UInt8} where {N}) && length(x) == p.schema.size)
end

function accepts(p::EnumPlan, x)
    return (x isa EnumValue && x.schema.name == p.schema.name) ||
           (x isa AbstractString && haskey(p.schema.symbolindex, x)) ||
           (x isa Symbol && haskey(p.schema.symbolindex, String(x)))
end

function accepts(p::EnumPlan, x::Base.Enum)
    symbol = avrosymbol(typeof(x), x)
    return symbol isa AbstractString && haskey(p.schema.symbolindex, symbol)
end

function accepts(::DatePlan, x)
    return x isa Date
end

function accepts(::Union{TimeMillisPlan,TimeMicrosPlan}, x)
    return x isa Time
end

function accepts(::TimestampPlan{P}, x) where {P}
    return x isa Durations.ZonedTimestamp || x isa DateTime
end

function accepts(::LocalTimestampPlan{P}, x) where {P}
    return x isa LocalTimestamp || x isa DateTime
end

function accepts(p::DecimalPlan, x)
    return (x isa DataDecimals.AbstractDecimal ? DataDecimals.scale(x) == p.scale : x isa WideDecimal && x.scale == p.scale)
end

function accepts(::UUIDStringPlan, x)
    return x isa UUID || (x isa AbstractString && tryparse(UUID, x) !== nothing)
end

function accepts(::UUIDFixedPlan, x)
    return x isa UUID
end

function accepts(::DurationPlan, x)
    return x isa Union{Duration,Durations.Duration}
end

# Iterable in Julia, but not Avro arrays.
const NOT_ARRAYLIKE = Union{AbstractString,AbstractDict,Number,AbstractChar,NamedTuple,Pair,Record}

function accepts(::WArray, x)
    return !(x isa NOT_ARRAYLIKE) && applicable(iterate, x)
end

function accepts(::WMap, x)
    return x isa AbstractDict || x isa NamedTuple
end

function accepts(::WUnion, x)
    return false
end

function accepts(p::WRecord, x)
    return x isa Record ? getfield(x, :schema).name == p.schema.name : (x isa NamedTuple || x isa AbstractDict || isrecordlike(x))
end

# Plain structs are record-like unless they are one of the scalar/container kinds the other branches own.
const NOT_RECORDLIKE = Union{Missing,Nothing,Number,AbstractString,Symbol,Char,AbstractArray,AbstractDict,Tuple,AbstractSet,Type,Function,
                             Dates.TimeType,UUID,Decimal,WideDecimal,Duration,Durations.Duration,UnionValue,Base.Enum}

function isrecordlike(x)
    return isstructtype(typeof(x)) && !(x isa NOT_RECORDLIKE)
end

# ---- records ---------------------------------------------------------------------------------------------

function encodevalue(p::WRecord, e::Encoder, x)
    enter!(e)
    encoderecord(p, e, x)
    leave!(e)
    return nothing
end

# A missing field is reported at the field's own path, with the field's schema expected.
@noinline function lacksfield(e::Encoder, field::Field, msg::AbstractString, x)
    pushencodepath!(e, ENCODE_PATH_FIELD, field.name, 0)
    e.expected = field.schema
    encodeerror(msg, x)
end

function encoderecord(p::WRecord, e::Encoder, x::Record)
    xs = getfield(x, :schema)
    vals = getfield(x, :values)
    length(vals) == length(xs.fields) || encodeerror("record value has $(length(vals)) values for $(length(xs.fields)) fields", x)
    aligned = xs === p.schema
    if !aligned && length(xs.fields) == length(p.schema.fields)
        aligned = xs.name == p.schema.name
        for i in eachindex(p.fields)
            aligned || break
            aligned = xs.fields[i].name == p.schema.fields[i].name
        end
    end
    if aligned
        for (i, f) in enumerate(p.fields)
            field = p.schema.fields[i]
            encodeframed(f, e, vals[i], field.schema, ENCODE_PATH_FIELD, field.name)
        end
        return nothing
    end
    for (i, f) in enumerate(p.schema.fields)
        j = get(xs.fieldindex, p.schema.fieldnames[i], 0)
        j == 0 && lacksfield(e, f, "record value lacks field \"$(f.name)\"", x)
        encodeframed(p.fields[i], e, vals[j], f.schema, ENCODE_PATH_FIELD, f.name)
    end
    return nothing
end

function encoderecord(p::WRecord, e::Encoder, x::AbstractDict)
    values = recorddictvalues(p, x)
    for (i, v) in enumerate(values)
        field = p.schema.fields[i]
        v === DICT_MISSING && lacksfield(e, field, "record value lacks field \"$(field.name)\"", x)
        encodeframed(p.fields[i], e, v, field.schema, ENCODE_PATH_FIELD, field.name)
    end
    return nothing
end

function encoderecord(p::WRecord, e::Encoder, x::T) where {T}
    x isa Tables.AbstractRow && return encoderow(p, e, x)
    (x isa NamedTuple || (isstructtype(T) && !(x isa AbstractArray) && !(x isa AbstractString))) || encodeerror("expected a record value for $(fullname(p.schema))", x)
    layout = recordlayout(p, T)
    layout.properties && return encoderow(p, e, x)
    positions = layout.positions
    for (i, f) in enumerate(p.fields)
        j = positions[i]
        field = p.schema.fields[i]
        j == 0 && lacksfield(e, field, "$(T) has no field for \"$(field.name)\" of record $(fullname(p.schema))", x)
        encodeframed(f, e, getfield(x, j), field.schema, ENCODE_PATH_FIELD, field.name)
    end
    return nothing
end

function recordlayout(p::WRecord, ::Type{T}) where {T}
    cached = @atomic p.layout
    cached !== nothing && cached.type === T && return cached
    properties = usesproperties(T)
    layout = RecordLayout(T, properties ? Int[] : recordfieldpositions(p.schema, T), properties)
    @atomic p.layout = layout
    return layout
end

# A row from `Tables.rows` that implements the row interface without subtyping `Tables.AbstractRow`
# (`DataFrames.DataFrameRow`, …); wrapping it routes field access through `Tables.getcolumn`.
struct TableRow{R}
    row::R
end

function encoderecord(p::WRecord, e::Encoder, x::TableRow)
    return encoderow(p, e, x.row)
end

function encoderow(p::WRecord, e::Encoder, row)
    positions = recordrowpositions(p, row)
    for (i, f) in enumerate(p.fields)
        j = positions[i]
        field = p.schema.fields[i]
        j == 0 && lacksfield(e, field, "row lacks column \"$(field.name)\"", row)
        encodeframed(f, e, Tables.getcolumn(row, j), field.schema, ENCODE_PATH_FIELD, field.name)
    end
    return nothing
end

const DICT_MISSING = Val(:missing_field)

# The dictionary's value for each record field (`DICT_MISSING` when absent; the first match wins).
function recorddictvalues(p::WRecord, x::AbstractDict)
    values = fill!(Vector{Any}(undef, length(p.fields)), DICT_MISSING)
    isempty(values) && return values
    for (k, v) in x
        (k isa Symbol || k isa AbstractString) ||
            encodeerror("record dictionary keys must be strings or symbols", k)
        i = recordfieldindex(p.schema, k)
        (i == 0 || values[i] !== DICT_MISSING) && continue
        values[i] = v
    end
    return values
end

function recordfieldindex(s::RecordSchema, name::Symbol)
    return get(s.fieldindex, name, 0)
end

function recordfieldindex(s::RecordSchema, name::AbstractString)
    return get(s.fieldindex, fieldsymbol(String(name)), 0)
end

# The column of each record field among `row`'s columns (0 when absent; the first match wins).
function recordrowpositions(p::WRecord, row)
    isempty(p.fields) && return Int[]
    names = Tables.columnnames(row)
    alignednames(p, names) && return collect(1:length(p.fields))
    positions = zeros(Int, length(p.fields))
    for (j, candidate) in enumerate(names)
        (candidate isa Symbol || candidate isa AbstractString) ||
            encodeerror("row column names must be strings or symbols", candidate)
        i = recordfieldindex(p.schema, candidate)
        i == 0 || positions[i] != 0 || (positions[i] = j)
    end
    return positions
end

# The Julia field of `T` holding each record field, by name or StructUtils `name` tag (0 when absent).
function recordfieldpositions(s::RecordSchema, ::Type{T}) where {T}
    positions = zeros(Int, length(s.fields))
    isempty(positions) && return positions
    tags = T <: NamedTuple ? (;) : StructUtils.fieldtags(AvroStyle(), T)
    for j in 1:fieldcount(T)
        fname = fieldname(T, j)
        fname isa Symbol || encodeerror("record fields must have names", fname)
        tagged = T <: NamedTuple ? nothing : fieldtag(tags, fname, :name)
        candidate = tagged === nothing ? fname : tagged
        (candidate isa Symbol || candidate isa AbstractString) ||
            throw(ArgumentError("StructUtils field tag `name` must be a Symbol or string, got $(typeof(candidate))"))
        i = recordfieldindex(s, candidate)
        i == 0 || positions[i] != 0 || (positions[i] = j)
    end
    return positions
end

# Whether the column names spell the record's field names in schema order.
function alignednames(p::WRecord, names)
    length(names) == length(p.fields) || return false
    for (i, n) in enumerate(names)
        same = n isa Symbol ? n === p.schema.fieldnames[i] : (n isa AbstractString && n == p.schema.fields[i].name)
        same || return false
    end
    return true
end
