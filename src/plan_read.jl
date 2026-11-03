# Generic read plans: one plan node per schema node, producing the generic value model. Every node is one
# of a closed set of types, and decoding dispatches on them through closed `isa` chains, so untrusted
# schemas never drive compilation or run-time method lookup.

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

function ArrayPlan(items::Plan, @nospecialize(eltype::Type), minsize::Int)
    return ArrayPlan(items, minsize, emptyvector(eltype))
end

struct MapPlan <: Plan
    values::Plan
    minsize::Int
    empty::Vector             # of the value type, which the decoded `Dict{String,V}` takes
end

function MapPlan(values::Plan, @nospecialize(eltype::Type), minsize::Int)
    return MapPlan(values, minsize, emptyvector(eltype))
end

# An empty `Vector{E}` for an `E` known at run time, allocated by the runtime (a `Vector{E}()` call would
# dispatch on `E`).
function emptyvector(@nospecialize(E::Type))
    return ccall(:jl_alloc_array_1d, Any, (Any, Csize_t), Vector{E}, 0)::Vector
end

struct UnionPlan <: Plan
    branches::Vector{Plan}
    nullable::Int        # position of the null branch in the two-branch nullable form; 0 otherwise
end

mutable struct RecordPlan <: Plan
    const schema::RecordSchema
    const fields::Vector{Plan}   # filled after memoisation so recursive references resolve
end

# The generic read plan of a schema, memoised per node id so recursive records share one `RecordPlan`.
# `mode`: `:little` reads legacy little-endian decimals; `:framing` reads every logical type as its
# underlying encoding, so only the framing of a datum is checked (`Avro.inspect`).
function readplan(@nospecialize(s::Schema); mode::Symbol=:standard)
    return readplan(s, Vector{Union{Nothing,Plan}}(nothing, graphinfo(s).nodes), mode)
end

function readplan(@nospecialize(s::Schema), memo::Vector{Union{Nothing,Plan}}, mode::Symbol)
    id = Int(nodeid(s)) + 1
    p = memo[id]
    p === nothing || return p
    p = buildreadplan(s, memo, mode)
    memo[id] = p
    return p
end

function buildreadplan(@nospecialize(s::Schema), memo::Vector{Union{Nothing,Plan}}, mode::Symbol)
    s isa NullSchema && return NullPlan()
    s isa BooleanSchema && return BoolPlan()
    s isa FloatSchema && return FloatPlan()
    s isa DoubleSchema && return DoublePlan()
    if s isa IntSchema
        mode === :framing && return IntPlan()
        s.logical isa DateLogical && return DatePlan()
        s.logical isa TimeMillis && return TimeMillisPlan()
        return IntPlan()
    elseif s isa LongSchema
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
    elseif s isa BytesSchema
        mode === :framing && return BytesPlan()
        l = s.logical
        l isa DecimalLogical && return DecimalPlan(0, l.precision, l.scale, l.precision > 38, mode === :little)
        return BytesPlan()
    elseif s isa StringSchema
        mode === :framing && return StringPlan()
        s.logical isa UUIDLogical && return UUIDStringPlan()
        return StringPlan()
    elseif s isa FixedSchema
        mode === :framing && return FixedPlan(s)
        l = s.logical
        l isa DecimalLogical && return DecimalPlan(s.size, l.precision, l.scale, l.precision > 38, mode === :little)
        l isa UUIDLogical && return UUIDFixedPlan()
        l isa DurationLogical && return DurationPlan()
        return FixedPlan(s)
    elseif s isa EnumSchema
        return EnumPlan(s)
    elseif s isa ArraySchema
        return ArrayPlan(readplan(s.items, memo, mode), elementtype(s.items), minsize(s.items))
    elseif s isa MapSchema
        return MapPlan(readplan(s.values, memo, mode), elementtype(s.values), minsize(s.values))
    elseif s isa UnionSchema
        return UnionPlan(Plan[readplan(b, memo, mode) for b in s.branches], nullablebranch(s))
    end
    r = s::RecordSchema
    p = RecordPlan(r, Plan[])
    memo[Int(nodeid(r)) + 1] = p
    for f in r.fields
        push!(p.fields, readplan(f.schema, memo, mode))
    end
    return p
end

# ---- decoding ---------------------------------------------------------------------------------------

# One value of `p` in the generic value model, counted against the decoder's value cap. A reader default
# consumes no input and is not counted: it comes from the caller's reader schema, not the file.
function decode(@nospecialize(p::Plan), d::Decoder)
    p isa DefaultPlan && return freshvalue(p.value)
    countvalue!(d)
    return decodevalue(p, d)
end

# The generic value of `p`, through a closed `isa` chain over the plan node types (the most common first).
function decodevalue(@nospecialize(p::Plan), d::Decoder)
    p isa LongPlan && return decodeleaf(p, d)
    p isa StringPlan && return decodeleaf(p, d)
    p isa UnionPlan && return decodeunion(p, d)
    p isa DoublePlan && return decodeleaf(p, d)
    p isa IntPlan && return decodeleaf(p, d)
    p isa BoolPlan && return decodeleaf(p, d)
    p isa RecordPlan && return decoderecord(p, d)
    p isa ArrayPlan && return decodearray(p, d)
    p isa MapPlan && return decodemap(p, d)
    p isa NullPlan && return decodeleaf(p, d)
    p isa FloatPlan && return decodeleaf(p, d)
    p isa BytesPlan && return decodeleaf(p, d)
    p isa EnumPlan && return decodeleaf(p, d)
    p isa FixedPlan && return decodeleaf(p, d)
    p isa DatePlan && return decodeleaf(p, d)
    p isa TimestampPlan{Millisecond} && return decodeleaf(p, d)
    p isa TimestampPlan{Microsecond} && return decodeleaf(p, d)
    p isa TimestampPlan{Nanosecond} && return decodeleaf(p, d)
    p isa LocalTimestampPlan{Millisecond} && return decodeleaf(p, d)
    p isa LocalTimestampPlan{Microsecond} && return decodeleaf(p, d)
    p isa LocalTimestampPlan{Nanosecond} && return decodeleaf(p, d)
    p isa TimeMillisPlan && return decodeleaf(p, d)
    p isa TimeMicrosPlan && return decodeleaf(p, d)
    p isa DecimalPlan && return decodeleaf(p, d)
    p isa UUIDStringPlan && return decodeleaf(p, d)
    p isa UUIDFixedPlan && return decodeleaf(p, d)
    p isa DurationPlan && return decodeleaf(p, d)
    return decoderesolving(p, d)
end

decodeleaf(::NullPlan, d::Decoder) = missing
decodeleaf(::BoolPlan, d::Decoder) = readbool(d)
decodeleaf(::IntPlan, d::Decoder) = readint(d)
decodeleaf(::LongPlan, d::Decoder) = readlong(d)
decodeleaf(::FloatPlan, d::Decoder) = readfloat(d)
decodeleaf(::DoublePlan, d::Decoder) = readdouble(d)
decodeleaf(::BytesPlan, d::Decoder) = readbytes(d)
decodeleaf(::StringPlan, d::Decoder) = readstring(d)

function decodeleaf(p::FixedPlan, d::Decoder)
    return Fixed(p.schema, readfixed(d, p.schema.size), Val(:unchecked))
end

function decodeleaf(p::EnumPlan, d::Decoder)
    return EnumValue(p.schema, Int32(readindex(d, length(p.schema.symbols))), Val(:unchecked))
end

const DATE_EPOCH = Date(1970, 1, 1)

function decodeleaf(::DatePlan, d::Decoder)
    return DATE_EPOCH + Day(readint(d))
end

function decodeleaf(::TimeMillisPlan, d::Decoder)
    v = readint(d)
    0 <= v < 86_400_000 || dataerror(d, "time-millis value $v out of range")
    return Time(Nanosecond(Int64(v) * 1_000_000))
end

function decodeleaf(::TimeMicrosPlan, d::Decoder)
    return timemicros(d, readlong(d))
end

function timemicros(d::Decoder, v::Int64)
    0 <= v < 86_400_000_000 || dataerror(d, "time-micros value $v out of range")
    return Time(Nanosecond(v * 1_000))
end

function decodeleaf(::TimestampPlan{P}, d::Decoder) where {P}
    return timestamp(P, readlong(d))
end

function decodeleaf(::LocalTimestampPlan{P}, d::Decoder) where {P}
    return localtimestamp(P, readlong(d))
end

function decodeleaf(p::DecimalPlan, d::Decoder)
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

function decodeleaf(::UUIDStringPlan, d::Decoder)
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

function decodeleaf(::UUIDFixedPlan, d::Decoder)
    skipfixed(d, 16)
    v = UInt128(0)
    @inbounds for i in 0:15
        v = (v << 8) | UInt128(d.buf[d.pos - 16 + i])
    end
    return UUID(v)
end

function decodeleaf(::DurationPlan, d::Decoder)
    skipfixed(d, 12)
    p = d.pos - 12
    function le32(i)
        return UInt32(d.buf[p + i]) | (UInt32(d.buf[p + i + 1]) << 8) | (UInt32(d.buf[p + i + 2]) << 16) | (UInt32(d.buf[p + i + 3]) << 24)
    end
    return Duration(le32(0), le32(4), le32(8))
end

# The items of one array or map value, `item(d)` decoding each: a count is a claim the remaining bytes must
# be able to hold, and a sized block must be consumed exactly.
@inline function eachblockitem(item::F, out, d::Decoder, minsize::Int, what::String) where {F}
    while true
        count, size = readblockcount(d)
        count == 0 && return out
        checkcount(d, count, minsize)
        isempty(out) && sizehint!(out, min(count, 1024))                           # a count is a claim until decoded
        outer = d.stop
        size >= 0 && (d.stop = d.pos + size - 1)
        for _ in 1:count
            item(d)
        end
        size >= 0 && (d.pos == d.stop + 1 || dataerror(d, "sized $what block not exactly consumed"))
        d.stop = outer
    end
end

function decodearray(p::ArrayPlan, d::Decoder)
    enter!(d)
    out = decodeitems(p.empty, p.items, p.minsize, d)
    leave!(d)
    return out
end

# A function barrier on the element type, so items are stored without boxing (the element types range over
# the closed value set).
function decodeitems(::Vector{T}, @nospecialize(items::Plan), minsize::Int, d::Decoder) where {T}
    out = T[]
    return eachblockitem(dd -> push!(out, decode(items, dd)), out, d, minsize, "array")
end

function decodemap(p::MapPlan, d::Decoder)
    enter!(d)
    out = decodeentries(p.empty, p.values, p.minsize, d)
    leave!(d)
    return out
end

# A function barrier like `decodeitems`; a repeated key keeps its last value.
function decodeentries(::Vector{V}, @nospecialize(values::Plan), minsize::Int, d::Decoder) where {V}
    out = Dict{String,V}()
    return eachblockitem(out, d, satadd(minsize, 1), "map") do dd
        k = readstring(dd)
        out[k] = decode(values, dd)
    end
end

# A nullable union counts its non-null branch value as `decode` would.
function decodeunion(p::UnionPlan, d::Decoder)
    n = length(p.branches)
    n == 0 && dataerror(d, "empty union has no datum")
    i = readindex(d, n)
    p.nullable == 0 || i != p.nullable || (countvalue!(d); return missing)
    return decode(p.branches[i], d)
end

function decoderecord(p::RecordPlan, d::Decoder)
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
function skip(@nospecialize(p::Plan), d::Decoder)
    p isa DefaultPlan && return nothing
    countvalue!(d)
    return skipvalue(p, d)
end

function skipvalue(@nospecialize(p::Plan), d::Decoder)
    p isa Union{StringPlan,BytesPlan} && return skiplen(d)
    p isa LongPlan && return (readlong(d); nothing)
    p isa UnionPlan && return skipunion(p, d)
    p isa DoublePlan && return (readdouble(d); nothing)
    p isa IntPlan && return (readint(d); nothing)
    p isa BoolPlan && return (readbool(d); nothing)
    p isa NullPlan && return nothing
    p isa RecordPlan && return skiprecord(p, d)
    p isa ArrayPlan && return skipblocks(p.items, p.minsize, d)
    p isa MapPlan && return skipblocks(p.values, satadd(p.minsize, 1), d, true)
    p isa FloatPlan && return (readfloat(d); nothing)
    p isa FixedPlan && return skipfixed(d, p.schema.size)
    p isa UUIDStringPlan && return skipuuid(d)
    p isa WrapPlan && return skipvalue(p.inner, d)
    p isa UnionResolvePlan && return skipresolvedunion(p, d)
    p isa ResolvedRecordPlan && return skipresolved(p, d)
    return skipleaf(p, d)
end

# The other leaves are skipped by decoding them, which validates them as a decode would.
function skipleaf(@nospecialize(p::Plan), d::Decoder)
    p isa EnumPlan && return (readindex(d, length(p.schema.symbols)); nothing)
    p isa DatePlan && return (readint(d); nothing)
    p isa Union{TimestampPlan,LocalTimestampPlan} && return (readlong(d); nothing)
    p isa TimeMillisPlan && return (decodeleaf(p, d); nothing)
    p isa TimeMicrosPlan && return (decodeleaf(p, d); nothing)
    p isa DecimalPlan && return (decodeleaf(p, d); nothing)
    p isa UUIDFixedPlan && return skipfixed(d, 16)
    p isa DurationPlan && return skipfixed(d, 12)
    p isa PromotePlan && return (decodepromoted(p, d); nothing)
    p isa EnumRemapPlan && return (enumremapindex(p, d); nothing)
    p isa UnresolvableBranch && throw(ResolutionError(p.msg, p.writerpath, p.readerpath))
    throw(ArgumentError("internal error: a plan without a skip"))
end

function skipuuid(d::Decoder)
    n = readlen(d)
    validuuid(d.buf, d.pos, n) || dataerror(d, "invalid uuid string ($n bytes)")
    d.pos += n
    return nothing
end

# Array (`keyed=false`) or map blocks: sized blocks are jumped, their declared items counted.
function skipblocks(@nospecialize(item::Plan), minsize::Int, d::Decoder, keyed::Bool=false)
    enter!(d)
    while true
        count, size = readblockcount(d)
        count == 0 && break
        checkcount(d, count, minsize)
        if size >= 0
            countvalues!(d, count)
            d.pos += size
        else
            for _ in 1:count
                keyed && skiplen(d)
                skip(item, d)
            end
        end
    end
    leave!(d)
    return nothing
end

function skipunion(p::UnionPlan, d::Decoder)
    n = length(p.branches)
    n == 0 && dataerror(d, "empty union has no datum")
    i = readindex(d, n)
    p.nullable == 0 || i != p.nullable || (countvalue!(d); return nothing)
    return skip(p.branches[i], d)
end

function skiprecord(p::RecordPlan, d::Decoder)
    enter!(d)
    for f in p.fields
        skip(f, d)
    end
    leave!(d)
    return nothing
end
