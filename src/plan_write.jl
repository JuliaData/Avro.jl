# Write plans (plan §4.5, §4.6 branch recovery, §4.3 validation): one dynamic node per schema node;
# values are extracted from generic values, NamedTuples, StructUtils structs, Tables rows,
# dictionaries and iterables.

abstract type WritePlan end

struct WNull <: WritePlan end
struct WBool <: WritePlan end
struct WInt <: WritePlan end
struct WLong <: WritePlan end
struct WFloat <: WritePlan end
struct WDouble <: WritePlan end
struct WBytes <: WritePlan end
struct WString <: WritePlan end
struct WFixed <: WritePlan
    schema::FixedSchema
end
struct WEnum <: WritePlan
    schema::EnumSchema
end
struct WDate <: WritePlan end
struct WTimeMillis <: WritePlan end
struct WTimeMicros <: WritePlan end
struct WTimestamp{P} <: WritePlan end
struct WLocalTimestamp{P} <: WritePlan end
struct WDecimal <: WritePlan
    fixedsize::Int
    precision::Int
    scale::Int
end
struct WUUIDString <: WritePlan end
struct WUUIDFixed <: WritePlan end
struct WDuration <: WritePlan end
struct WArray <: WritePlan
    items::WritePlan
end
struct WMap <: WritePlan
    values::WritePlan
end
struct WUnion <: WritePlan
    schema::UnionSchema
    branches::Vector{WritePlan}
    nullable::Int
    reprtypes::Vector{Any}           # juliatype of each branch (for exact representation matching)
end
mutable struct WRecord <: WritePlan
    const schema::RecordSchema
    const fields::Vector{WritePlan}
    const fieldmaps::Vector{Pair{Any,Vector{Int}}}   # per source type: Julia field positions for each Avro field (0 = absent)
end

function writeplan(s::Schema; budget::Union{Nothing,Budget}=nothing)
    nodes = graphinfo(s).nodes
    mbytes = vectorbytes(Union{Nothing,WritePlan}, nodes)
    budget === nothing || reserve!(budget, mbytes)     # the construction memo, released once the root is built
    memo = Vector{Union{Nothing,WritePlan}}(nothing, nodes)
    budget === nothing || allocated!(budget, mbytes)
    p = writeplan(s, memo, budget)
    budget === nothing || release!(budget, mbytes)     # the memo dies here; the plan graph stays charged
    return p
end

function writeplan(s::Schema, memo, budget)
    id = Int(nodeid(s)) + 1
    p = memo[id]
    p === nothing || return p
    budget === nothing || addresolution!(budget, 1)
    p = buildwriteplan(s, memo, budget)
    memo[id] = p
    return p
end

function buildwriteplan(::NullSchema, memo, budget)
    return WNull()
end

function buildwriteplan(::BooleanSchema, memo, budget)
    return WBool()
end

function buildwriteplan(s::IntSchema, memo, budget)
    s.logical isa DateLogical && return WDate()
    s.logical isa TimeMillis && return WTimeMillis()
    return WInt()
end
function buildwriteplan(s::LongSchema, memo, budget)
    l = s.logical
    l isa TimeMicros && return WTimeMicros()
    l isa TimestampMillis && return WTimestamp{Millisecond}()
    l isa TimestampMicros && return WTimestamp{Microsecond}()
    l isa TimestampNanos && return WTimestamp{Nanosecond}()
    l isa LocalTimestampMillis && return WLocalTimestamp{Millisecond}()
    l isa LocalTimestampMicros && return WLocalTimestamp{Microsecond}()
    l isa LocalTimestampNanos && return WLocalTimestamp{Nanosecond}()
    return WLong()
end

function buildwriteplan(::FloatSchema, memo, budget)
    return WFloat()
end

function buildwriteplan(::DoubleSchema, memo, budget)
    return WDouble()
end

function buildwriteplan(s::BytesSchema, memo, budget)
    s.logical isa DecimalLogical && return WDecimal(0, s.logical.precision, s.logical.scale)
    return WBytes()
end
function buildwriteplan(s::StringSchema, memo, budget)
    s.logical isa UUIDLogical && return WUUIDString()
    return WString()
end
function buildwriteplan(s::FixedSchema, memo, budget)
    l = s.logical
    l isa DecimalLogical && return WDecimal(s.size, l.precision, l.scale)
    l isa UUIDLogical && return WUUIDFixed()
    l isa DurationLogical && return WDuration()
    return WFixed(s)
end

function buildwriteplan(s::EnumSchema, memo, budget)
    return WEnum(s)
end

function buildwriteplan(s::ArraySchema, memo, budget)
    return WArray(writeplan(s.items, memo, budget))
end

function buildwriteplan(s::MapSchema, memo, budget)
    return WMap(writeplan(s.values, memo, budget))
end

function buildwriteplan(s::UnionSchema, memo, budget)
    n = length(s.branches)
    vb = 2 * vectorbytes(Any, n) + 64
    budget === nothing || reserve!(budget, vb)         # the exact branch and type vectors and node shell (§4.4)
    branches = Vector{WritePlan}(undef, n)
    jtypes = Vector{Any}(undef, n)
    budget === nothing || allocated!(budget, vb)
    for (i, b) in enumerate(s.branches)
        branches[i] = writeplan(b, memo, budget)
        jtypes[i] = juliatype(b)
    end
    return WUnion(s, branches, nullablebranch(s), jtypes)
end
function buildwriteplan(s::RecordSchema, memo, budget)
    nf = length(s.fields)
    slots = vectorbytes(WritePlan, nf) + 88            # the exact field vector, the fieldmap cache shell and node shell
    budget === nothing || reserve!(budget, slots)
    fields = Vector{WritePlan}(undef, nf)
    resize!(fields, 0)
    p = WRecord(s, fields, Pair{Any,Vector{Int}}[])
    budget === nothing || allocated!(budget, slots)
    memo[Int(nodeid(s)) + 1] = p
    for f in s.fields
        push!(p.fields, writeplan(f.schema, memo, budget))
    end
    return p
end

# ---- errors ---------------------------------------------------------------------------------------------

function encodeerror(msg::AbstractString, x)
    throw(EncodeError(string(msg, " (got ", typeof(x), ")"), "", nothing))
end

# ---- encoding --------------------------------------------------------------------------------------------

"""
    encode(plan, e::Encoder, x)

Encode `x` under `plan` into `e`, validating it against the schema and charging values to the budget.
"""
function encode(p::WritePlan, e::Encoder, x)
    b = e.budget
    if b.values >= b.workcap && e.pos > e.credited
        addinput!(b, e.pos - e.credited)               # encoded bytes are the encode side's work denominator
        e.credited = e.pos
    end
    countvalues!(b)
    encodevalue(p, e, x)
    return nothing
end

function encodevalue(::WNull, e::Encoder, x::Union{Missing,Nothing})
    return nothing
end

function encodevalue(::WNull, e::Encoder, x)
    return encodeerror("expected null (missing or nothing)", x)
end

function encodevalue(::WBool, e::Encoder, x::Bool)
    return writebool!(e, x)
end

function encodevalue(::WBool, e::Encoder, x)
    return encodeerror("expected a Bool", x)
end

function encodevalue(::WInt, e::Encoder, x::Integer)
    typemin(Int32) <= x <= typemax(Int32) || encodeerror("integer $x does not fit an Avro int", x)
    writeint!(e, Int32(x))
    return nothing
end

function encodevalue(::WInt, e::Encoder, x::Bool)
    return encodeerror("expected an integer", x)
end

function encodevalue(::WInt, e::Encoder, x)
    return encodeerror("expected an integer", x)
end

function encodevalue(::WLong, e::Encoder, x::Integer)
    typemin(Int64) <= x <= typemax(Int64) || encodeerror("integer $x does not fit an Avro long", x)
    writelong!(e, Int64(x))
    return nothing
end

function encodevalue(::WLong, e::Encoder, x::Bool)
    return encodeerror("expected an integer", x)
end

function encodevalue(::WLong, e::Encoder, x)
    return encodeerror("expected an integer", x)
end

function encodevalue(::WFloat, e::Encoder, x::Union{Float32,Float16})
    return writefloat!(e, Float32(x))
end

function encodevalue(::WFloat, e::Encoder, x::Integer)
    return x isa Bool ? encodeerror("expected a Float32", x) : writefloat!(e, Float32(x))
end

function encodevalue(::WFloat, e::Encoder, x)
    return encodeerror("expected a Float32 (a Float64 is not accepted for an Avro float)", x)
end

function encodevalue(::WDouble, e::Encoder, x::AbstractFloat)
    return writedouble!(e, Float64(x))
end

function encodevalue(::WDouble, e::Encoder, x::Integer)
    return x isa Bool ? encodeerror("expected a Float64", x) : writedouble!(e, Float64(x))
end

function encodevalue(::WDouble, e::Encoder, x)
    return encodeerror("expected a Float64", x)
end

function encodevalue(::WBytes, e::Encoder, x::AbstractVector{UInt8})
    return writebytes!(e, x)
end

function encodevalue(::WBytes, e::Encoder, x)
    return encodeerror("expected bytes (an AbstractVector{UInt8})", x)
end

function encodevalue(::WString, e::Encoder, x::AbstractString)
    s = x isa String ? x : String(x)
    isstrictutf8(s) || encodeerror("string is not valid UTF-8", x)
    writestring!(e, s)
    return nothing
end

function encodevalue(p::WString, e::Encoder, x::Symbol)
    return encodevalue(p, e, String(x))
end

function encodevalue(p::WString, e::Encoder, x::Char)
    return encodevalue(p, e, string(x))
end

function encodevalue(::WString, e::Encoder, x)
    return encodeerror("expected a string", x)
end

function encodevalue(p::WFixed, e::Encoder, x::Fixed)
    (fullname(x.schema) == fullname(p.schema) && x.schema.size == p.schema.size) || encodeerror("fixed value of $(fullname(x.schema)) does not match $(fullname(p.schema))", x)
    length(x.bytes) == p.schema.size || encodeerror("fixed $(fullname(p.schema)) needs exactly $(p.schema.size) bytes, got $(length(x.bytes))", x)
    writeraw!(e, x.bytes)
    return nothing
end
function encodevalue(p::WFixed, e::Encoder, x::AbstractVector{UInt8})
    length(x) == p.schema.size || encodeerror("fixed $(fullname(p.schema)) needs exactly $(p.schema.size) bytes, got $(length(x))", x)
    writeraw!(e, x)
    return nothing
end
function encodevalue(p::WFixed, e::Encoder, x::NTuple{N,UInt8}) where {N}
    N == p.schema.size || encodeerror("fixed $(fullname(p.schema)) needs exactly $(p.schema.size) bytes, got $N", x)
    ensureroom!(e, N)
    for b in x
        writebyte!(e, b)
    end
    return nothing
end

function encodevalue(p::WFixed, e::Encoder, x)
    return encodeerror("expected $(p.schema.size) fixed bytes", x)
end

function encodevalue(p::WEnum, e::Encoder, x::EnumValue)
    if x.schema === p.schema || (fullname(x.schema) == fullname(p.schema) && x.schema.symbols.data == p.schema.symbols.data)
        writelong!(e, Int64(x.index) - 1)
        return nothing
    end
    return encodesymbol(p, e, String(x))
end

function encodevalue(p::WEnum, e::Encoder, x::AbstractString)
    return encodesymbol(p, e, String(x))
end

function encodevalue(p::WEnum, e::Encoder, x::Symbol)
    return encodesymbol(p, e, String(x))
end

function encodevalue(p::WEnum, e::Encoder, x::Base.Enum)
    return encodesymbol(p, e, avrosymbol(typeof(x), x))
end

function encodevalue(p::WEnum, e::Encoder, x)
    return encodeerror("expected an enum symbol of $(fullname(p.schema))", x)
end

function encodesymbol(p::WEnum, e::Encoder, sym::String)
    i = get(p.schema.symbolindex, sym, 0)
    i == 0 && encodeerror("\"$(escapename(sym))\" is not a symbol of enum $(fullname(p.schema))", sym)
    writelong!(e, Int64(i) - 1)
    return nothing
end

function encodevalue(::WDate, e::Encoder, x::Date)
    return writeint!(e, Int32(Dates.value(x - DATE_EPOCH)))
end

function encodevalue(::WDate, e::Encoder, x)
    return encodeerror("expected a Date", x)
end

function encodevalue(::WTimeMillis, e::Encoder, x::Time)
    ns = Dates.value(x)
    ns % 1_000_000 == 0 || encodeerror("time-millis needs a millisecond-aligned Time (use Avro.truncate/Avro.round)", x)
    writeint!(e, Int32(ns ÷ 1_000_000))
    return nothing
end

function encodevalue(::WTimeMillis, e::Encoder, x)
    return encodeerror("expected a Time", x)
end

function encodevalue(::WTimeMicros, e::Encoder, x::Time)
    ns = Dates.value(x)
    ns % 1_000 == 0 || encodeerror("time-micros needs a microsecond-aligned Time (use Avro.truncate/Avro.round)", x)
    writelong!(e, ns ÷ 1_000)
    return nothing
end

function encodevalue(::WTimeMicros, e::Encoder, x)
    return encodeerror("expected a Time", x)
end

function encodevalue(::WTimestamp{P}, e::Encoder, x::Timestamp{P}) where {P}
    return writelong!(e, x.ticks)
end

function encodevalue(::WTimestamp{P}, e::Encoder, x::DateTime) where {P}
    return writelong!(e, Timestamp{P}(x).ticks)
end

function encodevalue(::WTimestamp{P}, e::Encoder, x) where {P}
    return encodeerror("expected an Avro.Timestamp{$(nameof(P))} or a DateTime", x)
end

function encodevalue(::WLocalTimestamp{P}, e::Encoder, x::LocalTimestamp{P}) where {P}
    return writelong!(e, x.ticks)
end

function encodevalue(::WLocalTimestamp{P}, e::Encoder, x::DateTime) where {P}
    return writelong!(e, LocalTimestamp{P}(x).ticks)
end

function encodevalue(::WLocalTimestamp{P}, e::Encoder, x) where {P}
    return encodeerror("expected an Avro.LocalTimestamp{$(nameof(P))} or a DateTime", x)
end

function encodevalue(p::WDecimal, e::Encoder, x::Decimal)
    x.scale == p.scale || encodeerror("decimal scale $(x.scale) does not equal the schema scale $(p.scale) (rescale first)", x)
    ndigits128(x.unscaled) <= p.precision || encodeerror("decimal exceeds precision $(p.precision)", x)
    return writetwoscomplement!(e, p, BigInt(x.unscaled))
end
function encodevalue(p::WDecimal, e::Encoder, x::WideDecimal)
    x.scale == p.scale || encodeerror("decimal scale $(x.scale) does not equal the schema scale $(p.scale) (rescale first)", x)
    ndigits(abs(x.unscaled)) <= p.precision || encodeerror("decimal exceeds precision $(p.precision)", x)
    return writetwoscomplement!(e, p, x.unscaled)
end

function encodevalue(p::WDecimal, e::Encoder, x)
    return encodeerror("expected an Avro.Decimal/WideDecimal with scale $(p.scale)", x)
end

"""
    twoscomplement(v::BigInt) -> Vector{UInt8}

The minimal big-endian two's complement representation of `v`.
"""
function twoscomplement(v::BigInt)
    if v >= 0
        nbytes = max(1, (ndigits(v; base=2) + 8) ÷ 8)   # room for the sign bit
        out = Vector{UInt8}(undef, nbytes)
        t = v
        for i in nbytes:-1:1
            out[i] = UInt8(t & 0xff)
            t >>= 8
        end
        return out
    end
    nbytes = max(1, (ndigits(-v - 1; base=2) + 8) ÷ 8)
    t = v + (BigInt(1) << (8 * nbytes))
    out = Vector{UInt8}(undef, nbytes)
    for i in nbytes:-1:1
        out[i] = UInt8(t & 0xff)
        t >>= 8
    end
    return out
end

function writetwoscomplement!(e::Encoder, p::WDecimal, v::BigInt)
    bytes = twoscomplement(v)
    if p.fixedsize == 0
        writebytes!(e, bytes)
        return nothing
    end
    length(bytes) <= p.fixedsize || encodeerror("decimal does not fit a fixed of $(p.fixedsize) bytes", v)
    pad = v < 0 ? 0xff : 0x00
    ensureroom!(e, p.fixedsize)
    for _ in 1:p.fixedsize - length(bytes)
        writebyte!(e, pad)
    end
    writeraw!(e, bytes)
    return nothing
end

function uuidbytes(u::UUID)
    v = UInt128(u)
    out = Vector{UInt8}(undef, 16)
    for i in 1:16
        out[i] = UInt8((v >> (8 * (16 - i))) & 0xff)
    end
    return out
end

function encodevalue(::WUUIDString, e::Encoder, x::UUID)
    return writestring!(e, string(x))
end

function encodevalue(::WUUIDString, e::Encoder, x::AbstractString)
    tryparseuuid(x) === nothing && encodeerror("not an RFC 4122 uuid string", x)
    writestring!(e, x)
    return nothing
end

function encodevalue(::WUUIDString, e::Encoder, x)
    return encodeerror("expected a UUID", x)
end

function encodevalue(::WUUIDFixed, e::Encoder, x::UUID)
    return writeraw!(e, uuidbytes(x))
end

function encodevalue(::WUUIDFixed, e::Encoder, x)
    return encodeerror("expected a UUID", x)
end

function encodevalue(::WDuration, e::Encoder, x::Duration)
    ensureroom!(e, 12)
    for v in (x.months, x.days, x.millis)
        for i in 0:3
            writebyte!(e, UInt8((v >> (8 * i)) & 0xff))
        end
    end
    return nothing
end

function encodevalue(::WDuration, e::Encoder, x)
    return encodeerror("expected an Avro.Duration", x)
end

# arrays: positive-count blocks (one block per array)
function encodevalue(p::WArray, e::Encoder, x)
    items = arrayitems(x)
    enter!(e)
    n = length(items)
    checkblockcount(e, n)
    if n > 0
        writelong!(e, Int64(n))
        for v in items
            encode(p.items, e, v)
        end
    end
    writelong!(e, Int64(0))
    leave!(e)
    return nothing
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
    Base.IteratorSize(x) isa Union{Base.HasLength,Base.HasShape} && return x
    if applicable(iterate, x)
        return collect(x)
    end
    return encodeerror("expected an array (an iterable collection)", x)
end

function encodevalue(p::WMap, e::Encoder, x)
    pairs = mappairs(x)
    enter!(e)
    n = length(pairs)
    checkblockcount(e, n)
    if n > 0
        writelong!(e, Int64(n))
        seen = String[]
        for (k, v) in pairs
            ks = mapkeystring(k)
            isstrictutf8(ks) || encodeerror("map key is not valid UTF-8", k)
            admitmapkey!(seen, ks, e.budget, k)
            writestring!(e, ks)
            encode(p.values, e, v)
        end
    end
    writelong!(e, Int64(0))
    leave!(e)
    return nothing
end

function checkblockcount(e::Encoder, n::Int)
    limit = e.budget.limits.max_block_count
    n <= limit || throw(limiterror(e.budget, :max_block_count, n, limit))
    return nothing
end

function mappairs(x::Map)
    return x
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

function mapkeystring(k::AbstractString)
    return String(k)
end

function mapkeystring(k::Symbol)
    return String(k)
end

function mapkeystring(k)
    return encodeerror("map keys must be strings or symbols", k)
end

function admitmapkey!(seen::Vector{String}, key::String, budget::Budget, source)
    for prior in seen
        addcompare!(budget, min(sizeof(prior), sizeof(key)) + 1)
        prior == key && encodeerror("map keys are duplicates after conversion to strings", source)
    end
    push!(seen, key)
    return nothing
end

# unions: branch recovery (plan §4.6)
function encodevalue(p::WUnion, e::Encoder, x)
    i = selectbranch(p, x)
    writelong!(e, Int64(i) - 1)
    encodevalue(p.branches[i], e, x isa UnionValue ? x.value : x)
    return nothing
end

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
    if x isa IDENTITY_VALUES
        s = schema(x)
        for (i, b) in enumerate(p.schema.branches)
            b isa NamedSchema || continue
            fullname(b) == fullname(s) || continue
            (b isa FixedSchema && s isa FixedSchema && b.size != s.size) && continue
            return i
        end
        encodeerror("no union branch has the identity $(fullname(s))", x)
    end
    T = typeof(x)
    for (i, r) in enumerate(p.reprtypes)
        r === T && return i
    end
    for (i, b) in enumerate(p.branches)
        accepts(b, x) && return i
    end
    encodeerror("no union branch accepts the value", x)
end

function accepts(::WNull, x)
    return x === missing || x === nothing
end

function accepts(::WBool, x)
    return x isa Bool
end

function accepts(::WInt, x)
    return x isa Integer && !(x isa Bool) && typemin(Int32) <= x <= typemax(Int32)
end

function accepts(::WLong, x)
    return x isa Integer && !(x isa Bool) && typemin(Int64) <= x <= typemax(Int64)
end

function accepts(::WFloat, x)
    return x isa Union{Float32,Float16} || (x isa Integer && !(x isa Bool))
end

function accepts(::WDouble, x)
    return x isa AbstractFloat || (x isa Integer && !(x isa Bool))
end

function accepts(::WBytes, x)
    return x isa AbstractVector{UInt8}
end

function accepts(::WString, x)
    return x isa AbstractString || x isa Symbol || x isa Char
end

function accepts(p::WFixed, x)
    return (x isa Fixed && fullname(x.schema) == fullname(p.schema)) || ((x isa AbstractVector{UInt8} || x isa NTuple{N,UInt8} where {N}) && length(x) == p.schema.size)
end

function accepts(p::WEnum, x)
    return (x isa EnumValue && fullname(x.schema) == fullname(p.schema)) || ((x isa AbstractString || x isa Symbol) && haskey(p.schema.symbolindex, String(x))) || x isa Base.Enum
end

function accepts(::WDate, x)
    return x isa Date
end

function accepts(::Union{WTimeMillis,WTimeMicros}, x)
    return x isa Time
end

function accepts(::WTimestamp{P}, x) where {P}
    return x isa Timestamp{P} || x isa DateTime
end

function accepts(::WLocalTimestamp{P}, x) where {P}
    return x isa LocalTimestamp{P} || x isa DateTime
end

function accepts(p::WDecimal, x)
    return (x isa Decimal || x isa WideDecimal) && x.scale == p.scale
end

function accepts(::WUUIDString, x)
    return x isa UUID || (x isa AbstractString && tryparseuuid(x) !== nothing)
end

function accepts(::WUUIDFixed, x)
    return x isa UUID
end

function accepts(::WDuration, x)
    return x isa Duration
end

function accepts(::WArray, x)
    return (x isa AbstractVector && !(x isa AbstractVector{UInt8})) || x isa Tuple || x isa AbstractSet
end

function accepts(::WMap, x)
    return x isa Map || x isa AbstractDict || x isa NamedTuple
end

function accepts(::WUnion, x)
    return false
end

function accepts(p::WRecord, x)
    return x isa Record ? fullname(getfield(x, :schema)) == fullname(p.schema) : (x isa NamedTuple || x isa AbstractDict || isrecordlike(x))
end

# Plain structs are record-like unless they are one of the scalar/container kinds the other branches own.
const NOT_RECORDLIKE = Union{Missing,Nothing,Number,AbstractString,Symbol,Char,AbstractArray,Tuple,AbstractSet,Type,Function,
                             Date,Time,DateTime,UUID,Decimal,WideDecimal,Timestamp,LocalTimestamp,Duration,Fixed,EnumValue,Map,UnionValue,Base.Enum}
function isrecordlike(x)
    return isstructtype(typeof(x)) && !(x isa NOT_RECORDLIKE)
end

# records: value extraction by Avro field name without interning untrusted names
function encodevalue(p::WRecord, e::Encoder, x)
    enter!(e)
    encoderecord(p, e, x)
    leave!(e)
    return nothing
end

function encoderecord(p::WRecord, e::Encoder, x::Record)
    xs = getfield(x, :schema)
    vals = getfield(x, :values)
    length(vals) == length(xs.fields) || encodeerror("record value has $(length(vals)) values for $(length(xs.fields)) fields", x)
    if xs === p.schema || (fullname(xs) == fullname(p.schema) && length(xs.fields) == length(p.schema.fields) && all(i -> xs.fields[i].name == p.schema.fields[i].name, eachindex(p.fields)))
        for (i, f) in enumerate(p.fields)
            encode(f, e, vals[i])
        end
        return nothing
    end
    for (i, f) in enumerate(p.schema.fields)
        j = get(xs.fieldindex, f.name, 0)
        j == 0 && encodeerror("record value lacks field \"$(f.name)\"", x)
        encode(p.fields[i], e, vals[j])
    end
    return nothing
end

function encoderecord(p::WRecord, e::Encoder, x::AbstractDict)
    for (i, f) in enumerate(p.schema.fields)
        v = dictfield(x, f.name)
        v === DICT_MISSING && encodeerror("record value lacks field \"$(f.name)\"", x)
        encode(p.fields[i], e, v)
    end
    return nothing
end

const DICT_MISSING = Val(:missing_field)

function dictfield(x::AbstractDict, name::String)
    K = keytype(x)
    if K <: AbstractString || K === Any
        v = get(x, name, DICT_MISSING)
        v === DICT_MISSING || return v
    end
    for (k, v) in x
        (k isa Symbol || k isa AbstractString) && String(k) == name && return v
    end
    return DICT_MISSING
end

function encoderecord(p::WRecord, e::Encoder, x::T) where {T}
    if x isa Tables.AbstractRow
        for (i, f) in enumerate(p.schema.fields)
            encode(p.fields[i], e, rowfield(x, f.name))
        end
        return nothing
    end
    (x isa NamedTuple || (isstructtype(T) && !(x isa AbstractArray) && !(x isa AbstractString))) || encodeerror("expected a record value for $(fullname(p.schema))", x)
    positions = fieldpositions(p, T)
    for (i, f) in enumerate(p.fields)
        j = positions[i]
        j == 0 && encodeerror("$(T) has no field for \"$(p.schema.fields[i].name)\" of record $(fullname(p.schema))", x)
        encode(f, e, getfield(x, j))
    end
    return nothing
end

"""
A row yielded by `Tables.rows` that satisfies the row interface without subtyping
`Tables.AbstractRow` (`DataFrames.DataFrameRow`, …): `Avro.write` wraps it so field access goes
through the Tables interface instead of `getfield`.
"""
struct TableRow{R}
    row::R
end

function encoderecord(p::WRecord, e::Encoder, x::TableRow)
    for (i, f) in enumerate(p.schema.fields)
        encode(p.fields[i], e, rowfield(x.row, f.name))
    end
    return nothing
end

function rowfield(row, name::String)
    for c in Tables.columnnames(row)
        String(c) == name && return Tables.getcolumn(row, c)
    end
    encodeerror("row lacks column \"$name\"", row)
end

"""
    fieldpositions(plan, T) -> Vector{Int}

For each Avro field of the record plan, the position of the Julia field of `T` with the same name
(honouring StructUtils `name` tags); computed once per `T` by comparing field-name strings (no
interning of schema names).
"""
function fieldpositions(p::WRecord, ::Type{T}) where {T}
    for (t, v) in p.fieldmaps
        t === T && return v
    end
    names = String[]
    tags = T <: NamedTuple ? (;) : StructUtils.fieldtags(AvroStyle(), T)
    for fname in fieldnames(T)
        tagged = T <: NamedTuple ? nothing : fieldtag(tags, fname, :name)
        push!(names, tagged === nothing ? string(fname) : String(tagged))
    end
    positions = Int[]
    for f in p.schema.fields
        j = findfirst(==(f.name), names)
        push!(positions, j === nothing ? 0 : j)
    end
    push!(p.fieldmaps, T => positions)
    return positions
end

# ---- the aligned-NamedTuple fast path (Phase 4d performance work) ------------------------------------
#
# `Avro.write` streams Tables rows, most commonly NamedTuples whose fields align 1:1 with the record
# plan. A per-writer cache holds the plan fields as a concrete tuple; the estimate and encode walks then
# run the *same* per-plan functions monomorphized through tuple recursion — identical arithmetic and
# validation, without per-field dynamic dispatch or boxing.

"The plan-field tuple for `T`, or `nothing` when `T` is not an aligned NamedTuple of the record."
function alignedplans(p::WritePlan, ::Type{T}) where {T}
    p isa WRecord || return nothing
    T <: NamedTuple || return nothing
    n = length(p.fields)
    (0 < n <= 32 && fieldcount(T) == n) || return nothing
    fieldpositions(p, T) == 1:n || return nothing
    return Tuple(p.fields)
end

@inline function estfields(::Tuple{}, ::Tuple{}, slack::Union{Nothing,Vector{Int}}, i::Int)
    return (0, 0, 0)
end

@inline function estfields(plans::Tuple, vals::Tuple, slack::Union{Nothing,Vector{Int}}, i::Int)
    eb, ev = estimatevalue(first(plans), first(vals))
    pb = slack === nothing ? 0 : max(eb - slack[i], 0)
    reb, rev, rpb = estfields(Base.tail(plans), Base.tail(vals), slack, i + 1)
    return (checked_add(eb, reb), ev + rev, checked_add(pb, rpb))
end

"The §4.9 estimate of one aligned row (the `estimaterootrecord` arithmetic, monomorphized)."
function estimatealigned(plans::Tuple, x::NamedTuple, slack::Union{Nothing,Vector{Int}})
    eb, ev, pb = estfields(plans, values(x), slack, 1)
    return (checked_add(recordbytes(length(plans)), eb), 1 + ev, pb)
end

@inline function encfields(e::Encoder, ::Tuple{}, ::Tuple{})
    return nothing
end

@inline function encfields(e::Encoder, plans::Tuple, vals::Tuple)
    encode(first(plans), e, first(vals))
    return encfields(e, Base.tail(plans), Base.tail(vals))
end

"""
One aligned row through the same `encode` methods, monomorphized: the record-level prelude of
`encode(::WritePlan, e, x)` plus `encodedatum!`'s datum-size check, then the fields in schema order.
"""
function encodealigned!(plans::Tuple, e::Encoder, x::NamedTuple)
    start = e.pos
    b = e.budget
    if b.values >= b.workcap && e.pos > e.credited
        addinput!(b, e.pos - e.credited)               # encoded bytes are the encode side's work denominator
        e.credited = e.pos
    end
    countvalues!(b)                                    # the record value itself
    encfields(e, plans, values(x))
    n = e.pos - start
    n <= b.limits.max_datum_bytes || throw(LimitError(:max_datum_bytes, n, b.limits.max_datum_bytes, :max_datum_bytes, :encode))
    return nothing
end
