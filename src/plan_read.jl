# Generic read plans (plan §4.5): one dynamic plan node per schema node, producing the finite value
# set of §4.6. Untrusted schemas never drive compilation — every plan node is one of a closed set of
# types and children are dispatched behind a function barrier.

abstract type ReadPlan end

struct NullPlan <: ReadPlan end
struct BoolPlan <: ReadPlan end
struct IntPlan <: ReadPlan end
struct LongPlan <: ReadPlan end
struct FloatPlan <: ReadPlan end
struct DoublePlan <: ReadPlan end
struct BytesPlan <: ReadPlan end
struct StringPlan <: ReadPlan end
struct FixedPlan <: ReadPlan
    schema::FixedSchema
end
struct EnumPlan <: ReadPlan
    schema::EnumSchema
end
struct DatePlan <: ReadPlan end
struct TimeMillisPlan <: ReadPlan end
struct TimeMicrosPlan <: ReadPlan end
struct TimestampPlan{P} <: ReadPlan end
struct LocalTimestampPlan{P} <: ReadPlan end
struct DecimalPlan <: ReadPlan
    fixedsize::Int       # 0 for `bytes`
    precision::Int
    scale::Int
    wide::Bool
    little::Bool         # decimal_byteorder=:little — Avro.jl ≤ 1.1.2 wrote native-endian decimals
end

DecimalPlan(fixedsize::Int, precision::Int, scale::Int, wide::Bool) = DecimalPlan(fixedsize, precision, scale, wide, false)
struct UUIDStringPlan <: ReadPlan end
struct UUIDFixedPlan <: ReadPlan end
struct DurationPlan <: ReadPlan end
struct ArrayPlan <: ReadPlan
    items::ReadPlan
    eltype::Type
    minsize::Int
end
struct MapPlan <: ReadPlan
    values::ReadPlan
    eltype::Type
    minsize::Int
end
struct UnionPlan <: ReadPlan
    branches::Vector{ReadPlan}
    nullable::Int        # position of the null branch in the two-branch nullable form; 0 otherwise
end
mutable struct RecordPlan <: ReadPlan
    const schema::RecordSchema
    const fields::Vector{ReadPlan}   # filled after registration so recursive references resolve
    const boxes::Vector{Int}         # boxed-value charge per field (0 for reference types)
end

"""
    readplan(schema) -> ReadPlan

The generic read plan of a schema (memoised per node id; recursion through shared `RecordPlan`s).
"""
function readplan(s::Schema; budget::Union{Nothing,Budget}=nothing)
    memo = Vector{Union{Nothing,ReadPlan}}(nothing, graphinfo(s).nodes)
    return readplan(s, memo, budget)
end

function readplan(s::Schema, memo::Vector{Union{Nothing,ReadPlan}}, budget)
    id = Int(nodeid(s)) + 1
    p = memo[id]
    p === nothing || return p
    budget === nothing || addresolution!(budget, 1)
    p = buildreadplan(s, memo, budget)
    memo[id] = p
    return p
end

buildreadplan(::NullSchema, memo, budget) = NullPlan()
buildreadplan(::BooleanSchema, memo, budget) = BoolPlan()
function buildreadplan(s::IntSchema, memo, budget)
    s.logical isa DateLogical && return DatePlan()
    s.logical isa TimeMillis && return TimeMillisPlan()
    return IntPlan()
end
function buildreadplan(s::LongSchema, memo, budget)
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
buildreadplan(::FloatSchema, memo, budget) = FloatPlan()
buildreadplan(::DoubleSchema, memo, budget) = DoublePlan()
function buildreadplan(s::BytesSchema, memo, budget)
    l = s.logical
    l isa DecimalLogical && return DecimalPlan(0, l.precision, l.scale, l.precision > 38)
    return BytesPlan()
end
function buildreadplan(s::StringSchema, memo, budget)
    s.logical isa UUIDLogical && return UUIDStringPlan()
    return StringPlan()
end
function buildreadplan(s::FixedSchema, memo, budget)
    l = s.logical
    l isa DecimalLogical && return DecimalPlan(s.size, l.precision, l.scale, l.precision > 38)
    l isa UUIDLogical && return UUIDFixedPlan()
    l isa DurationLogical && return DurationPlan()
    return FixedPlan(s)
end
buildreadplan(s::EnumSchema, memo, budget) = EnumPlan(s)
buildreadplan(s::ArraySchema, memo, budget) = ArrayPlan(readplan(s.items, memo, budget), elementtype(s.items), minsize(s.items))
buildreadplan(s::MapSchema, memo, budget) = MapPlan(readplan(s.values, memo, budget), elementtype(s.values), minsize(s.values))
function buildreadplan(s::UnionSchema, memo, budget)
    return UnionPlan(ReadPlan[readplan(b, memo, budget) for b in s.branches], nullablebranch(s))
end
function buildreadplan(s::RecordSchema, memo, budget)
    p = RecordPlan(s, ReadPlan[], Int[])
    memo[Int(nodeid(s)) + 1] = p
    for f in s.fields
        push!(p.fields, readplan(f.schema, memo, budget))
        push!(p.boxes, boxcharge(juliatype(f.schema)))
    end
    return p
end

# ---- decoding ---------------------------------------------------------------------------------------

"""
    decode(plan, d::Decoder)

Decode one value of `plan` from `d` into its generic representation, charging values and payload to the
decoder's budget.
"""
function decode(p::ReadPlan, d::Decoder)
    countvalues!(d.budget)
    return decodevalue(p, d)
end

decodevalue(::NullPlan, d::Decoder) = missing
decodevalue(::BoolPlan, d::Decoder) = readbool(d)
decodevalue(::IntPlan, d::Decoder) = readint(d)
decodevalue(::LongPlan, d::Decoder) = readlong(d)
decodevalue(::FloatPlan, d::Decoder) = readfloat(d)
decodevalue(::DoublePlan, d::Decoder) = readdouble(d)
decodevalue(::BytesPlan, d::Decoder) = readbytes(d)
decodevalue(::StringPlan, d::Decoder) = readstring(d)
function decodevalue(p::FixedPlan, d::Decoder)
    reserve!(d.budget, STORAGE[].fixed)
    return Fixed(p.schema, readfixed(d, p.schema.size), Val(:unchecked))
end
function decodevalue(p::EnumPlan, d::Decoder)
    i = readindex(d, length(p.schema.symbols))
    reserve!(d.budget, enumvaluebytes())
    return EnumValue(p.schema, Int32(i), Val(:unchecked))
end

const DATE_EPOCH = Date(1970, 1, 1)
decodevalue(::DatePlan, d::Decoder) = DATE_EPOCH + Day(readint(d))

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

decodevalue(::TimestampPlan{P}, d::Decoder) where {P} = Timestamp{P}(readlong(d))
decodevalue(::LocalTimestampPlan{P}, d::Decoder) where {P} = LocalTimestamp{P}(readlong(d))

function decodevalue(p::DecimalPlan, d::Decoder)
    if p.fixedsize == 0
        n = readlen(d, d.budget.limits.max_bytes, :max_bytes)
        n == 0 && dataerror(d, "empty decimal payload")
        start = d.pos
        d.pos += n
        return decimalfrombytes(d, p, start, n)
    end
    p.fixedsize == 0 && dataerror(d, "decimal on a zero-size fixed")
    skipfixed(d, p.fixedsize)
    return decimalfrombytes(d, p, d.pos - p.fixedsize, p.fixedsize)
end

"""
    decimalfrombytes(d, plan, start, n)

Big-endian two's complement of `n` bytes at `start` into `Decimal` (≤ 38 digits) or `WideDecimal`,
validating `digits(unscaled) ≤ precision` (spec's "maximum precision"; Java checks only on encode).
"""
function decimalfrombytes(d::Decoder, p::DecimalPlan, start::Int, n::Int)
    big = p.wide || n > 16
    big && reserve!(d.budget, widedecimalbytes(n))        # the BigInt path allocates before the value is known
    v = decimalunscaled(d, p, start, n)
    v isa Int128 || return WideDecimal(v, p.scale)
    big && release!(d.budget, widedecimalbytes(n))        # a narrow result computed through a transient BigInt
    return Decimal(v, p.scale)                            # isbits: charged where it is boxed or stored
end

"The validated unscaled value of the `n` bytes at `start`: an `Int128` for the narrow representation, a `BigInt` for the wide one."
function decimalunscaled(d::Decoder, p::DecimalPlan, start::Int, n::Int)
    buf = d.buf
    if !p.wide && n <= 16
        v = Int128(0)
        @inbounds for i in 0:n - 1
            v = (v << 8) | Int128(buf[p.little ? start + n - 1 - i : start + i])
        end
        shift = 8 * (16 - n)
        v = (v << shift) >> shift   # sign-extend
        ndigits128(v) <= p.precision || dataerror(d, "decimal exceeds precision $(p.precision)")
        return v
    end
    big = BigInt(0)
    @inbounds for i in 0:n - 1
        big = (big << 8) | BigInt(buf[p.little ? start + n - 1 - i : start + i])
    end
    if buf[p.little ? start + n - 1 : start] >= 0x80
        big -= BigInt(1) << (8 * n)
    end
    ndigits(abs(big)) <= p.precision || dataerror(d, "decimal exceeds precision $(p.precision)")
    p.wide && return big
    typemin(Int128) <= big <= typemax(Int128) || dataerror(d, "decimal exceeds precision $(p.precision)")
    return Int128(big)
end

function ndigits128(v::Int128)
    v == 0 && return 1
    v == typemin(Int128) && return 39
    a = abs(v)
    n = 0
    while a > 0
        a ÷= 10
        n += 1
    end
    return n
end

function decodevalue(::UUIDStringPlan, d::Decoder)
    n = readlen(d, d.budget.limits.max_bytes, :max_bytes)
    p = d.pos
    validuuid(d.buf, p, n) || dataerror(d, "invalid uuid string \"$(escapename(unsafe_substring(d.buf, p, n)))\"")
    d.pos = p + n
    return uuidfrombuffer(d.buf, p)
end

"RFC 4122 text `8-4-4-4-12` (hex digits in either case) at `buf[from:from + n - 1]`."
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

hexnibble(b::UInt8) = b <= UInt8('9') ? b - UInt8('0') : (b | 0x20) - UInt8('a') + 0x0a

function uuidfrombuffer(buf::AbstractVector{UInt8}, from::Int)
    v = UInt128(0)
    for i in 1:36
        i in (9, 14, 19, 24) && continue
        v = (v << 4) | UInt128(hexnibble(buf[from + i - 1]))
    end
    return UUID(v)
end

"""
    tryparseuuid(s) -> Union{Nothing,UUID}

RFC 4122 text `8-4-4-4-12` hex digits in either case; nothing otherwise.
"""
function tryparseuuid(s::AbstractString)
    cu = codeunits(s)
    length(cu) == 36 || return nothing
    for (i, b) in enumerate(cu)
        if i in (9, 14, 19, 24)
            b == UInt8('-') || return nothing
        else
            ishex8(b) || return nothing
        end
    end
    return UUID(s)
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
    le32(i) = UInt32(d.buf[p + i]) | (UInt32(d.buf[p + i + 1]) << 8) | (UInt32(d.buf[p + i + 2]) << 16) | (UInt32(d.buf[p + i + 3]) << 24)
    return Duration(le32(0), le32(4), le32(8))
end

# ---- growth rule: explicit exact-capacity replacement ---------------------------------------------

mutable struct GrowBuf{T}
    data::Vector{T}
    len::Int
end

function GrowBuf{T}(d::Decoder, hint::Int) where {T}
    cap = max(min(hint, 1024), 0)
    reserve!(d.budget, vectorbytes(T, cap))
    return GrowBuf{T}(Vector{T}(undef, cap), 0)
end

@inline function Base.push!(g::GrowBuf{T}, d::Decoder, x) where {T}
    if g.len == length(g.data)
        newcap = max(2 * length(g.data), 4)
        reserve!(d.budget, vectorbytes(T, newcap))
        nd = Vector{T}(undef, newcap)
        copyto!(nd, 1, g.data, 1, g.len)
        release!(d.budget, vectorbytes(T, length(g.data)))
        g.data = nd
    end
    g.len += 1
    @inbounds g.data[g.len] = x
    return g
end

function finish!(g::GrowBuf{T}, d::Decoder) where {T}
    out = g.data
    if g.len != length(out)
        reserve!(d.budget, vectorbytes(T, g.len))
        out = Vector{T}(undef, g.len)
        copyto!(out, 1, g.data, 1, g.len)
        release!(d.budget, vectorbytes(T, length(g.data)))
    end
    return out
end

function checkcount(d::Decoder, count::Int, minsize::Int)
    minsize <= 0 && return nothing
    minsize == INFINITE && count > 0 && dataerror(d, "block declares $count items of a schema with no finite datum")
    count <= remaining(d) ÷ minsize || dataerror(d, "block declares $count items but only $(remaining(d)) bytes remain")
    return nothing
end

function decodevalue(p::ArrayPlan, d::Decoder)
    enter!(d)
    first = true
    g = nothing
    while true
        count, size = readblockcount(d)
        count == 0 && break
        if size >= 0
            checkcount(d, count, p.minsize)
            stop = d.pos + size - 1
            g = decodeitems!(g, p, d, count, stop)
            d.pos == stop + 1 || dataerror(d, "sized array block not exactly consumed")
        else
            checkcount(d, count, p.minsize)
            g = decodeitems!(g, p, d, count, -1)
        end
        first = false
    end
    leave!(d)
    g === nothing && (reserve!(d.budget, STORAGE[].vector); return p.eltype === Any ? Any[] : Vector{p.eltype}(undef, 0))
    return finish!(g, d)
end

function decodeitems!(g, p::ArrayPlan, d::Decoder, count::Int, stop::Int)
    T = p.eltype
    g === nothing && (g = GrowBuf{T}(d, count))
    return fillitems!(g, p.items, d, count, stop)
end

function fillitems!(g::GrowBuf{T}, items::ReadPlan, d::Decoder, count::Int, stop::Int) where {T}
    outerstop = d.stop
    stop >= 0 && (stop <= d.stop || dataerror(d, "sized block exceeds the remaining bytes"); d.stop = stop)
    for _ in 1:count
        push!(g, d, decode(items, d))
    end
    d.stop = outerstop
    return g
end

function decodevalue(p::MapPlan, d::Decoder)
    enter!(d)
    ks = GrowBuf{String}(d, 0)
    vs = nothing
    while true
        count, size = readblockcount(d)
        count == 0 && break
        checkcount(d, count, p.minsize + 1)
        vs === nothing && (vs = GrowBuf{p.eltype}(d, count))
        outerstop = d.stop
        if size >= 0
            stop = d.pos + size - 1
            stop <= d.stop || dataerror(d, "sized map block exceeds the remaining bytes")
            d.stop = stop
        end
        for _ in 1:count
            k = readstring(d)
            push!(ks, d, k)
            push!(vs, d, decode(p.values, d))
        end
        size >= 0 && (d.pos == d.stop + 1 || dataerror(d, "sized map block not exactly consumed"))
        d.stop = outerstop
    end
    leave!(d)
    keys = finish!(ks, d)
    vals = vs === nothing ? Vector{p.eltype}(undef, 0) : finish!(vs, d)
    addinput!(d.budget, 0)
    return buildmap(p.eltype, keys, vals, d.budget)
end

function decodevalue(p::UnionPlan, d::Decoder)
    n = length(p.branches)
    n == 0 && dataerror(d, "empty union has no datum")
    i = readindex(d, n)
    if p.nullable != 0
        i == p.nullable && return missing
        return decodevalue(p.branches[i], d)
    end
    reserve!(d.budget, unionvaluebytes())
    v = decodevalue(p.branches[i], d)
    isbits(v) && reserve!(d.budget, boxbytes(typeof(v)))
    return UnionValue(i, v)
end

function decodevalue(p::RecordPlan, d::Decoder)
    enter!(d)
    n = length(p.fields)
    reserve!(d.budget, recordbytes(n))
    vals = Vector{Any}(undef, n)
    @inbounds for i in 1:n
        v = decode(p.fields[i], d)
        b = p.boxes[i]
        b > 0 && reserve!(d.budget, b)
        vals[i] = v
    end
    leave!(d)
    return Record(p.schema, vals, Val(:unchecked))
end

# ---- skipping ----------------------------------------------------------------------------------------

"""
    skip(plan, d::Decoder)

Skip one value: in `:strict` mode every framing element is walked and validated exactly as a full
decode (skipped strings are length-validated but not UTF-8-validated); in `:fast` mode sized array/map
blocks are jumped by their byte size.
"""
function skip(p::ReadPlan, d::Decoder)
    countvalues!(d.budget)
    return skipvalue(p, d)
end

skipvalue(::NullPlan, d::Decoder) = nothing
skipvalue(::BoolPlan, d::Decoder) = (readbool(d); nothing)
# Logical values are domain-checked when skipped exactly as when decoded (plan §4.3: only skipped
# strings are not UTF-8-validated).
skipvalue(::Union{IntPlan,DatePlan}, d::Decoder) = (readint(d); nothing)
skipvalue(p::TimeMillisPlan, d::Decoder) = (decodevalue(p, d); nothing)
skipvalue(::Union{LongPlan,TimestampPlan,LocalTimestampPlan}, d::Decoder) = (readlong(d); nothing)
skipvalue(p::TimeMicrosPlan, d::Decoder) = (decodevalue(p, d); nothing)
skipvalue(::FloatPlan, d::Decoder) = (readfloat(d); nothing)
skipvalue(::DoublePlan, d::Decoder) = (readdouble(d); nothing)
skipvalue(::Union{BytesPlan,StringPlan}, d::Decoder) = skiplen(d)
function skipvalue(::UUIDStringPlan, d::Decoder)
    n = readlen(d, d.budget.limits.max_bytes, :max_bytes)
    validuuid(d.buf, d.pos, n) || dataerror(d, "invalid uuid string \"$(escapename(unsafe_substring(d.buf, d.pos, n)))\"")
    d.pos += n
    return nothing
end
function skipvalue(p::DecimalPlan, d::Decoder)
    n = p.fixedsize == 0 ? readlen(d, d.budget.limits.max_bytes, :max_bytes) : p.fixedsize
    n == 0 && dataerror(d, "empty decimal payload")
    n <= remaining(d) || dataerror(d, "fixed of $n bytes exceeds the remaining $(remaining(d)) bytes")
    big = p.wide || n > 16
    big && reserve!(d.budget, widedecimalbytes(n))        # the transient BigInt of the check
    decimalunscaled(d, p, d.pos, n)
    big && release!(d.budget, widedecimalbytes(n))
    d.pos += n
    return nothing
end
skipvalue(p::FixedPlan, d::Decoder) = skipfixed(d, p.schema.size)
skipvalue(::UUIDFixedPlan, d::Decoder) = skipfixed(d, 16)
skipvalue(::DurationPlan, d::Decoder) = skipfixed(d, 12)
skipvalue(p::EnumPlan, d::Decoder) = (readindex(d, length(p.schema.symbols)); nothing)

function skipvalue(p::ArrayPlan, d::Decoder)
    enter!(d)
    while true
        count, size = readblockcount(d)
        count == 0 && break
        checkcount(d, count, p.minsize)
        if size >= 0 && d.validate === :fast
            size <= remaining(d) || dataerror(d, "sized block exceeds the remaining bytes")
            d.pos += size
            continue
        end
        outerstop = d.stop
        if size >= 0
            stop = d.pos + size - 1
            stop <= d.stop || dataerror(d, "sized block exceeds the remaining bytes")
            d.stop = stop
        end
        for _ in 1:count
            skip(p.items, d)
        end
        size >= 0 && (d.pos == d.stop + 1 || dataerror(d, "sized array block not exactly consumed"))
        d.stop = outerstop
    end
    leave!(d)
    return nothing
end

function skipvalue(p::MapPlan, d::Decoder)
    enter!(d)
    while true
        count, size = readblockcount(d)
        count == 0 && break
        checkcount(d, count, p.minsize + 1)
        if size >= 0 && d.validate === :fast
            size <= remaining(d) || dataerror(d, "sized block exceeds the remaining bytes")
            d.pos += size
            continue
        end
        outerstop = d.stop
        if size >= 0
            stop = d.pos + size - 1
            stop <= d.stop || dataerror(d, "sized block exceeds the remaining bytes")
            d.stop = stop
        end
        for _ in 1:count
            skiplen(d)
            skip(p.values, d)
        end
        size >= 0 && (d.pos == d.stop + 1 || dataerror(d, "sized map block not exactly consumed"))
        d.stop = outerstop
    end
    leave!(d)
    return nothing
end

function skipvalue(p::UnionPlan, d::Decoder)
    n = length(p.branches)
    n == 0 && dataerror(d, "empty union has no datum")
    i = readindex(d, n)
    return skipvalue(p.branches[i], d)
end

function skipvalue(p::RecordPlan, d::Decoder)
    enter!(d)
    for f in p.fields
        skip(f, d)
    end
    leave!(d)
    return nothing
end
