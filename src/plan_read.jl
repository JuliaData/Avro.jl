# Generic read plans: one plan node per schema node, producing the generic value model. Every node is one
# of a closed set of types, so untrusted schemas never drive compilation; children are dispatched behind a
# function barrier.

# Leaf plans serve reading and writing; composites have one node type each way.
abstract type Plan end

struct NullPlan <: Plan end
struct BoolPlan <: Plan end
struct IntPlan <: Plan end
struct LongPlan <: Plan end
struct FloatPlan <: Plan end
struct DoublePlan <: Plan end
struct BytesPlan <: Plan end
struct StringPlan <: Plan end
struct FixedPlan <: Plan
    schema::FixedSchema
end
struct EnumPlan <: Plan
    schema::EnumSchema
end
struct DatePlan <: Plan end
struct TimeMillisPlan <: Plan end
struct TimeMicrosPlan <: Plan end
struct TimestampPlan{P} <: Plan end
struct LocalTimestampPlan{P} <: Plan end
struct DecimalPlan <: Plan
    fixedsize::Int       # 0 for `bytes`
    precision::Int
    scale::Int
    wide::Bool
    little::Bool         # legacy=:avrojl1: Avro.jl ≤ 1.1.2 wrote decimals little-endian
end

function DecimalPlan(fixedsize::Int, precision::Int, scale::Int, wide::Bool)
    return DecimalPlan(fixedsize, precision, scale, wide, false)
end

struct UUIDStringPlan <: Plan end
struct UUIDFixedPlan <: Plan end
struct DurationPlan <: Plan end
# `empty` (and `MapPlan.empty`) is a never-mutated empty container of the element type: decoding dispatches
# on its type, because applying a runtime `eltype` per container costs microseconds for `Union` types.
struct ArrayPlan <: Plan
    items::Plan
    minsize::Int
    empty::Vector
end

function ArrayPlan(items::Plan, eltype::Type, minsize::Int)
    return ArrayPlan(items, minsize, Vector{eltype}())
end

struct MapPlan <: Plan
    values::Plan
    minsize::Int
    empty::Dict{String}
end

function MapPlan(values::Plan, eltype::Type, minsize::Int)
    return MapPlan(values, minsize, Dict{String,eltype}())
end

# `Q` is the concrete plan type of a nullable union's non-null branch (`Nothing` for other unions).
struct UnionPlan{Q} <: Plan
    branches::Vector{Plan}
    nullable::Int        # position of the null branch in the two-branch nullable form; 0 otherwise
end

function UnionPlan(branches::Vector{Plan}, nullable::Int)
    Q = nullable == 0 ? Nothing : typeof(branches[3 - nullable])
    return UnionPlan{Q}(branches, nullable)
end

mutable struct RecordPlan <: Plan
    const schema::RecordSchema
    const fields::Vector{Plan}   # filled after memoisation so recursive references resolve
end

# The generic read plan of a schema, memoised per node id so recursive records share one `RecordPlan`.
# `mode`: `:little` reads legacy little-endian decimals; `:framing` reads every logical type as its
# underlying encoding, so only the framing of a datum is checked (`Avro.inspect`).
function readplan(s::Schema; mode::Symbol=:standard)
    return readplan(s, Vector{Union{Nothing,Plan}}(nothing, graphinfo(s).nodes), mode)
end

function readplan(s::Schema, memo::Vector{Union{Nothing,Plan}}, mode::Symbol)
    id = Int(nodeid(s)) + 1
    p = memo[id]
    p === nothing || return p
    p = buildreadplan(s, memo, mode)
    memo[id] = p
    return p
end

function buildreadplan(::NullSchema, memo, mode)
    return NullPlan()
end

function buildreadplan(::BooleanSchema, memo, mode)
    return BoolPlan()
end

function buildreadplan(s::IntSchema, memo, mode)
    mode === :framing && return IntPlan()
    s.logical isa DateLogical && return DatePlan()
    s.logical isa TimeMillis && return TimeMillisPlan()
    return IntPlan()
end

function buildreadplan(s::LongSchema, memo, mode)
    mode === :framing && return LongPlan()
    l = s.logical
    l isa TimeMicros && return TimeMicrosPlan()
    l isa TimestampMillis && return TimestampPlan{Millisecond}()
    l isa TimestampMicros && return TimestampPlan{Microsecond}()
    l isa TimestampNanos && return TimestampPlan{Nanosecond}()
    l isa LocalTimestampMillis && return LocalTimestampPlan{Millisecond}()
    l isa LocalTimestampMicros && return LocalTimestampPlan{Microsecond}()
    l isa LocalTimestampNanos && return LocalTimestampPlan{Nanosecond}()
    return LongPlan()
end

function buildreadplan(::FloatSchema, memo, mode)
    return FloatPlan()
end

function buildreadplan(::DoubleSchema, memo, mode)
    return DoublePlan()
end

function buildreadplan(s::BytesSchema, memo, mode)
    mode === :framing && return BytesPlan()
    l = s.logical
    l isa DecimalLogical && return DecimalPlan(0, l.precision, l.scale, l.precision > 38, mode === :little)
    return BytesPlan()
end

function buildreadplan(s::StringSchema, memo, mode)
    mode === :framing && return StringPlan()
    s.logical isa UUIDLogical && return UUIDStringPlan()
    return StringPlan()
end

function buildreadplan(s::FixedSchema, memo, mode)
    mode === :framing && return FixedPlan(s)
    l = s.logical
    l isa DecimalLogical && return DecimalPlan(s.size, l.precision, l.scale, l.precision > 38, mode === :little)
    l isa UUIDLogical && return UUIDFixedPlan()
    l isa DurationLogical && return DurationPlan()
    return FixedPlan(s)
end

function buildreadplan(s::EnumSchema, memo, mode)
    return EnumPlan(s)
end

function buildreadplan(s::ArraySchema, memo, mode)
    return ArrayPlan(readplan(s.items, memo, mode), elementtype(s.items), minsize(s.items))
end

function buildreadplan(s::MapSchema, memo, mode)
    return MapPlan(readplan(s.values, memo, mode), elementtype(s.values), minsize(s.values))
end

function buildreadplan(s::UnionSchema, memo, mode)
    branches = Plan[readplan(b, memo, mode) for b in s.branches]
    return UnionPlan(branches, nullablebranch(s))
end

function buildreadplan(s::RecordSchema, memo, mode)
    p = RecordPlan(s, Plan[])
    memo[Int(nodeid(s)) + 1] = p
    for f in s.fields
        push!(p.fields, readplan(f.schema, memo, mode))
    end
    return p
end

# ---- decoding ---------------------------------------------------------------------------------------

# One value of `p` in the generic value model, counted against the decoder's value cap.
function decode(p::Plan, d::Decoder)
    countvalue!(d)
    return decodevalue(p, d)
end

function decodevalue(::NullPlan, d::Decoder)
    return missing
end

function decodevalue(::BoolPlan, d::Decoder)
    return readbool(d)
end

function decodevalue(::IntPlan, d::Decoder)
    return readint(d)
end

function decodevalue(::LongPlan, d::Decoder)
    return readlong(d)
end

function decodevalue(::FloatPlan, d::Decoder)
    return readfloat(d)
end

function decodevalue(::DoublePlan, d::Decoder)
    return readdouble(d)
end

function decodevalue(::BytesPlan, d::Decoder)
    return readbytes(d)
end

function decodevalue(::StringPlan, d::Decoder)
    return readstring(d)
end

function decodevalue(p::FixedPlan, d::Decoder)
    return Fixed(p.schema, readfixed(d, p.schema.size), Val(:unchecked))
end

function decodevalue(p::EnumPlan, d::Decoder)
    return EnumValue(p.schema, Int32(readindex(d, length(p.schema.symbols))), Val(:unchecked))
end

const DATE_EPOCH = Date(1970, 1, 1)

function decodevalue(::DatePlan, d::Decoder)
    return DATE_EPOCH + Day(readint(d))
end

function decodevalue(::TimeMillisPlan, d::Decoder)
    v = readint(d)
    0 <= v < 86_400_000 || dataerror(d, "time-millis value $v out of range")
    return Time(Nanosecond(Int64(v) * 1_000_000))
end

function decodevalue(::TimeMicrosPlan, d::Decoder)
    v = readlong(d)
    0 <= v < 86_400_000_000 || dataerror(d, "time-micros value $v out of range")
    return Time(Nanosecond(v * 1_000))
end

function decodevalue(::TimestampPlan{P}, d::Decoder) where {P}
    return timestamp(P, readlong(d))
end

function decodevalue(::LocalTimestampPlan{P}, d::Decoder) where {P}
    return localtimestamp(P, readlong(d))
end

function decodevalue(p::DecimalPlan, d::Decoder)
    n = p.fixedsize == 0 ? readlen(d) : p.fixedsize
    n == 0 && dataerror(d, "empty decimal payload")
    skipfixed(d, n)
    return decimalfrombytes(d, p, d.pos - n, n)
end

# The two's complement of the `n` bytes at `start` as a `Decimal` (≤ 38 digits) or `WideDecimal`, validating
# `digits(unscaled) ≤ precision` (the spec's maximum precision; Java checks only on encode).
function decimalfrombytes(d::Decoder, p::DecimalPlan, start::Int, n::Int)
    v = decimalunscaled(d, p, start, n)
    v isa Int128 || return WideDecimal(v, p.scale)
    return Decimal(v, p.scale)
end

# The checked unscaled value of the `n` bytes at `start`: an `Int128` for the narrow representation, a
# `BigInt` for the wide one.
function decimalunscaled(d::Decoder, p::DecimalPlan, start::Int, n::Int)
    buf = d.buf
    if !p.wide && n <= 16
        v = Int128(0)
        @inbounds for i in 0:n - 1
            v = (v << 8) | Int128(buf[p.little ? start + n - 1 - i : start + i])
        end
        shift = 8 * (16 - n)
        v = (v << shift) >> shift   # sign-extend
        ndigits(v) <= p.precision || dataerror(d, "decimal exceeds precision $(p.precision)")
        return v
    end
    bytes = view(buf, start:start + n - 1)
    big = parse(BigInt, bytes2hex(p.little ? reverse(bytes) : bytes); base=16)
    (p.little ? bytes[end] : bytes[1]) >= 0x80 && (big -= BigInt(1) << checked_mul(8, n))
    ndigits(big) <= p.precision || dataerror(d, "decimal exceeds precision $(p.precision)")
    return p.wide ? big : Int128(big)
end

function decodevalue(::UUIDStringPlan, d::Decoder)
    n = readlen(d)
    p = d.pos
    validuuid(d.buf, p, n) || dataerror(d, "invalid uuid string ($n bytes)")
    d.pos = p + n
    return uuidfrombuffer(d.buf, p)
end

# RFC 4122 text `8-4-4-4-12` (hex digits in either case) at `buf[from:from + n - 1]`.
function validuuid(buf::AbstractVector{UInt8}, from::Int, n::Int)
    n == 36 || return false
    for i in 1:36
        b = buf[from + i - 1]
        if i in (9, 14, 19, 24)
            b == UInt8('-') || return false
        else
            ishex8(b) || return false
        end
    end
    return true
end

function hexnibble(b::UInt8)
    return b <= UInt8('9') ? b - UInt8('0') : (b | 0x20) - UInt8('a') + 0x0a
end

function uuidfrombuffer(buf::AbstractVector{UInt8}, from::Int)
    v = UInt128(0)
    for i in 1:36
        i in (9, 14, 19, 24) && continue
        v = (v << 4) | UInt128(hexnibble(buf[from + i - 1]))
    end
    return UUID(v)
end

function decodevalue(::UUIDFixedPlan, d::Decoder)
    skipfixed(d, 16)
    v = UInt128(0)
    @inbounds for i in 0:15
        v = (v << 8) | UInt128(d.buf[d.pos - 16 + i])
    end
    return UUID(v)
end

function decodevalue(::DurationPlan, d::Decoder)
    skipfixed(d, 12)
    p = d.pos - 12
    function le32(i)
        return UInt32(d.buf[p + i]) | (UInt32(d.buf[p + i + 1]) << 8) | (UInt32(d.buf[p + i + 2]) << 16) | (UInt32(d.buf[p + i + 3]) << 24)
    end
    return Duration(le32(0), le32(4), le32(8))
end

function decodevalue(p::ArrayPlan, d::Decoder)
    enter!(d)
    out = decodeitems!(p.empty, p.items, p, d)
    leave!(d)
    return out
end

# A function barrier on the element type and the concrete item plan, so items decode without dynamic
# dispatch or boxing (both range over the closed plan and value sets).
function decodeitems!(::Vector{T}, items::Plan, p::ArrayPlan, d::Decoder) where {T}
    out = T[]
    while true
        count, size = readblockcount(d)
        count == 0 && return out
        checkcount(d, count, p.minsize)
        isempty(out) && sizehint!(out, min(count, 1024))                           # a count is a claim until decoded
        outer = d.stop
        size >= 0 && (d.stop = d.pos + size - 1)
        for _ in 1:count
            push!(out, decode(items, d))
        end
        size >= 0 && (d.pos == d.stop + 1 || dataerror(d, "sized array block not exactly consumed"))
        d.stop = outer
    end
end

function decodevalue(p::MapPlan, d::Decoder)
    enter!(d)
    out = decodeentries!(p.empty, p.values, p, d)
    leave!(d)
    return out
end

# A function barrier like `decodeitems!`; a repeated key keeps its last value.
function decodeentries!(::Dict{String,V}, values::Plan, p::MapPlan, d::Decoder) where {V}
    out = Dict{String,V}()
    while true
        count, size = readblockcount(d)
        count == 0 && return out
        checkcount(d, count, satadd(p.minsize, 1))
        isempty(out) && sizehint!(out, min(count, 1024))
        outer = d.stop
        size >= 0 && (d.stop = d.pos + size - 1)
        for _ in 1:count
            k = readstring(d)
            out[k] = decode(values, d)
        end
        size >= 0 && (d.pos == d.stop + 1 || dataerror(d, "sized map block not exactly consumed"))
        d.stop = outer
    end
end

# A nullable union decodes its non-null branch through the concrete `Q` (counting the branch value as
# `decode` does); calling `decode` here would dispatch on the abstract branch and lose that type.
function decodevalue(p::UnionPlan{Q}, d::Decoder) where {Q<:Plan}
    i = readindex(d, 2)
    countvalue!(d)
    i == p.nullable && return missing
    return decodevalue(p.branches[i]::Q, d)
end

function decodevalue(p::UnionPlan{Nothing}, d::Decoder)
    n = length(p.branches)
    n == 0 && dataerror(d, "empty union has no datum")
    return decode(p.branches[readindex(d, n)], d)
end

function decodevalue(p::RecordPlan, d::Decoder)
    enter!(d)
    vals = Vector{Any}(undef, length(p.fields))
    for i in eachindex(vals)
        @inbounds vals[i] = decode(p.fields[i], d)
    end
    leave!(d)
    return Record(p.schema, vals, Val(:unchecked))
end

# ---- skipping ----------------------------------------------------------------------------------------

# Skip one value, validating it as a decode would, except that skipped strings are length-checked but not
# UTF-8-checked and sized array/map blocks are jumped by their byte size (their declared items counted).
function skip(p::Plan, d::Decoder)
    countvalue!(d)
    return skipvalue(p, d)
end

# Leaves whose decoded value costs nothing skip by decoding it.
function skipvalue(p::Plan, d::Decoder)
    decodevalue(p, d)
    return nothing
end

function skipvalue(::Union{BytesPlan,StringPlan}, d::Decoder)
    return skiplen(d)
end

function skipvalue(::UUIDStringPlan, d::Decoder)
    n = readlen(d)
    validuuid(d.buf, d.pos, n) || dataerror(d, "invalid uuid string ($n bytes)")
    d.pos += n
    return nothing
end

function skipvalue(p::FixedPlan, d::Decoder)
    return skipfixed(d, p.schema.size)
end

function skipvalue(p::ArrayPlan, d::Decoder)
    enter!(d)
    while true
        count, size = readblockcount(d)
        count == 0 && break
        checkcount(d, count, p.minsize)
        if size >= 0
            countvalues!(d, count)
            d.pos += size
        else
            for _ in 1:count
                skip(p.items, d)
            end
        end
    end
    leave!(d)
    return nothing
end

function skipvalue(p::MapPlan, d::Decoder)
    enter!(d)
    while true
        count, size = readblockcount(d)
        count == 0 && break
        checkcount(d, count, satadd(p.minsize, 1))
        if size >= 0
            countvalues!(d, count)
            d.pos += size
        else
            for _ in 1:count
                skiplen(d)
                skip(p.values, d)
            end
        end
    end
    leave!(d)
    return nothing
end

function skipvalue(p::UnionPlan{Q}, d::Decoder) where {Q<:Plan}
    i = readindex(d, 2)
    countvalue!(d)
    i == p.nullable && return nothing
    return skipvalue(p.branches[i]::Q, d)
end

function skipvalue(p::UnionPlan{Nothing}, d::Decoder)
    n = length(p.branches)
    n == 0 && dataerror(d, "empty union has no datum")
    return skip(p.branches[readindex(d, n)], d)
end

function skipvalue(p::RecordPlan, d::Decoder)
    enter!(d)
    for f in p.fields
        skip(f, d)
    end
    leave!(d)
    return nothing
end
