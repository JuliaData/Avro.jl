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

function DecimalPlan(fixedsize::Int, precision::Int, scale::Int, wide::Bool)
    return DecimalPlan(fixedsize, precision, scale, wide, false)
end

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

"Build one plan node under §4.4 order: reserve its box, construct, settle."
function plannode(f, budget::Union{Nothing,Budget}, bytes::Int=64)
    budget === nothing || reserve!(budget, bytes)
    try
        p = f()
        budget === nothing || allocated!(budget, bytes)
        return p
    catch
        budget === nothing || unreserve!(budget, bytes)
        rethrow()
    end
end

"""
    readplan(schema) -> ReadPlan

The generic read plan of a schema (memoised per node id; recursion through shared `RecordPlan`s).
"""
function readplan(s::Schema; budget::Union{Nothing,Budget}=nothing, little::Bool=false)
    nodes = graphinfo(s).nodes
    mbytes = vectorbytes(Union{Nothing,ReadPlan}, nodes)
    checkpoint = budget === nothing ? nothing : budgetcheckpoint(budget)
    try
        budget === nothing || reserve!(budget, mbytes)     # the construction memo, released once the root is built
        memo = Vector{Union{Nothing,ReadPlan}}(nothing, nodes)
        budget === nothing || allocated!(budget, mbytes)
        p = readplan(s, memo, budget, little)
        budget === nothing || release!(budget, mbytes)     # the memo dies here; the plan graph stays charged
        return p
    catch
        budget === nothing || rollbackreservations!(budget, checkpoint::NTuple{2,Int})
        rethrow()
    end
end

function readplan(s::Schema, memo::Vector{Union{Nothing,ReadPlan}}, budget, little::Bool=false)
    id = Int(nodeid(s)) + 1
    p = memo[id]
    p === nothing || return p
    budget === nothing || addresolution!(budget, 1)
    p = buildreadplan(s, memo, budget, little)
    memo[id] = p
    return p
end

function buildreadplan(::NullSchema, memo, budget, little)
    return NullPlan()
end

function buildreadplan(::BooleanSchema, memo, budget, little)
    return BoolPlan()
end

function buildreadplan(s::IntSchema, memo, budget, little)
    s.logical isa DateLogical && return DatePlan()
    s.logical isa TimeMillis && return TimeMillisPlan()
    return IntPlan()
end

function buildreadplan(s::LongSchema, memo, budget, little)
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

function buildreadplan(::FloatSchema, memo, budget, little)
    return FloatPlan()
end

function buildreadplan(::DoubleSchema, memo, budget, little)
    return DoublePlan()
end

function buildreadplan(s::BytesSchema, memo, budget, little)
    l = s.logical
    l isa DecimalLogical && return plannode(() -> DecimalPlan(0, l.precision, l.scale, l.precision > 38, little), budget)
    return BytesPlan()
end

function buildreadplan(s::StringSchema, memo, budget, little)
    s.logical isa UUIDLogical && return UUIDStringPlan()
    return StringPlan()
end

function buildreadplan(s::FixedSchema, memo, budget, little)
    l = s.logical
    l isa DecimalLogical && return plannode(() -> DecimalPlan(s.size, l.precision, l.scale, l.precision > 38, little), budget)
    l isa UUIDLogical && return UUIDFixedPlan()
    l isa DurationLogical && return DurationPlan()
    return plannode(() -> FixedPlan(s), budget)
end

function buildreadplan(s::EnumSchema, memo, budget, little)
    return plannode(() -> EnumPlan(s), budget)
end

function buildreadplan(s::ArraySchema, memo, budget, little)
    items = readplan(s.items, memo, budget, little)
    ms = budget === nothing ? minsize(s.items) : minsize(s.items, budget)
    return plannode(() -> ArrayPlan(items, elementtype(s.items), ms), budget)
end

function buildreadplan(s::MapSchema, memo, budget, little)
    values = readplan(s.values, memo, budget, little)
    ms = budget === nothing ? minsize(s.values) : minsize(s.values, budget)
    return plannode(() -> MapPlan(values, elementtype(s.values), ms), budget)
end

function buildreadplan(s::UnionSchema, memo, budget, little)
    n = length(s.branches)
    vectors = vectorbytes(ReadPlan, n)
    node = 48
    budget === nothing || reserve!(budget, vectors + node)
    branches = Vector{ReadPlan}(undef, n)
    budget === nothing || allocated!(budget, vectors)
    for (i, b) in enumerate(s.branches)
        branches[i] = readplan(b, memo, budget, little)
    end
    p = UnionPlan(branches, nullablebranch(s))
    budget === nothing || allocated!(budget, node)
    return p
end

function buildreadplan(s::RecordSchema, memo, budget, little)
    nf = length(s.fields)
    slots = vectorbytes(ReadPlan, nf) + vectorbytes(Int, nf) + 64
    budget === nothing || reserve!(budget, slots)      # the exact field vectors and node shell (§4.4)
    fields = Vector{ReadPlan}(undef, nf)
    resize!(fields, 0)
    boxes = Vector{Int}(undef, nf)
    resize!(boxes, 0)
    p = RecordPlan(s, fields, boxes)
    budget === nothing || allocated!(budget, slots)
    memo[Int(nodeid(s)) + 1] = p
    for f in s.fields
        push!(p.fields, readplan(f.schema, memo, budget, little))
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
    reserve!(d.budget, STORAGE[].fixed)
    v = Fixed(p.schema, readfixed(d, p.schema.size), Val(:unchecked))
    allocated!(d.budget, STORAGE[].fixed)
    return v
end

function decodevalue(p::EnumPlan, d::Decoder)
    i = readindex(d, length(p.schema.symbols))
    reserve!(d.budget, enumvaluebytes())
    v = EnumValue(p.schema, Int32(i), Val(:unchecked))
    allocated!(d.budget, enumvaluebytes())
    return v
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
    return Timestamp{P}(readlong(d))
end

function decodevalue(::LocalTimestampPlan{P}, d::Decoder) where {P}
    return LocalTimestamp{P}(readlong(d))
end

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
    v = decimalunscaled(d, p, start, n)
    v isa Int128 || return WideDecimal(v, p.scale)
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
    charge = widedecimalbytes(n)
    checkpoint = budgetcheckpoint(d.budget)
    try
        reserve!(d.budget, charge)
        big = BigInt(; nbits=checked_mul(8, n))
        allocated!(d.budget, charge)
        negative = buf[p.little ? start + n - 1 : start] >= 0x80
        if negative
            tempcharge = bytesbytes(n)
            reserve!(d.budget, tempcharge)
            temp = Vector{UInt8}(undef, n)
            allocated!(d.budget, tempcharge)
            copyto!(temp, 1, buf, start, n)
            for i in eachindex(temp)
                @inbounds temp[i] = ~temp[i]
            end
            carry = true
            indices = p.little ? eachindex(temp) : reverse(eachindex(temp))
            for i in indices
                carry || break
                @inbounds value, carry = Base.add_with_overflow(temp[i], UInt8(1))
                @inbounds temp[i] = value
            end
            importdecimal!(big, temp, 1, n, p.little)
            Base.GMP.flipsign!(big, -1)
            release!(d.budget, tempcharge)
        else
            importdecimal!(big, buf, start, n, p.little)
        end
        ndigits(big) <= p.precision || dataerror(d, "decimal exceeds precision $(p.precision)")
        p.wide && return big
        typemin(Int128) <= big <= typemax(Int128) || dataerror(d, "decimal exceeds precision $(p.precision)")
        narrow = Int128(big)
        release!(d.budget, charge)
        return narrow
    catch
        rollbackreservations!(d.budget, checkpoint)
        rethrow()
    end
end

function importdecimal!(big::BigInt, bytes::AbstractVector{UInt8}, start::Int, n::Int, little::Bool)
    order = little ? -1 : 1
    GC.@preserve bytes big ccall((:__gmpz_import, Base.GMP.libgmp), Cvoid,
        (Ref{BigInt}, Csize_t, Cint, Csize_t, Cint, Csize_t, Ptr{UInt8}),
        big, n, order, 1, 1, 0, pointer(bytes, start))
    return big
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
    validuuid(d.buf, p, n) ||
        dataerror(d, diagnosticstring(d.budget, "invalid uuid string (", n, " bytes)"))
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
    function le32(i)
        return UInt32(d.buf[p + i]) | (UInt32(d.buf[p + i + 1]) << 8) | (UInt32(d.buf[p + i + 2]) << 16) | (UInt32(d.buf[p + i + 3]) << 24)
    end
    return Duration(le32(0), le32(4), le32(8))
end

# ---- growth rule: explicit exact-capacity replacement ---------------------------------------------

mutable struct GrowBuf{T}
    data::Vector{T}
    len::Int
end

function growinitialcapacity(hint::Int)
    return max(min(hint, 1024), 0)
end

function grownextcapacity(capacity::Int)
    return max(checked_mul(2, capacity), 4)
end

function GrowBuf{T}(d::Decoder, hint::Int) where {T}
    cap = growinitialcapacity(hint)
    charge = vectorbytes(T, cap) + shellbytes(GrowBuf{T})
    checkpoint = budgetcheckpoint(d.budget)
    data = nothing
    try
        reserve!(d.budget, charge)
        data = Vector{T}(undef, cap)
        g = GrowBuf{T}(data, 0)
        allocated!(d.budget, charge)
        return g
    catch
        data = nothing
        rollbackreservations!(d.budget, checkpoint)
        rethrow()
    end
end

@inline function Base.push!(g::GrowBuf{T}, d::Decoder, x) where {T}
    if g.len == length(g.data)
        newcap = grownextcapacity(length(g.data))
        reserve!(d.budget, vectorbytes(T, newcap))
        nd = Vector{T}(undef, newcap)
        allocated!(d.budget, vectorbytes(T, newcap))
        copyto!(nd, 1, g.data, 1, g.len)
        oldbytes = vectorbytes(T, length(g.data))
        g.data = nd                                       # the old storage is unreachable only after the rebind
        release!(d.budget, oldbytes)
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
        allocated!(d.budget, vectorbytes(T, g.len))
        copyto!(out, 1, g.data, 1, g.len)
        oldbytes = vectorbytes(T, length(g.data))
        g.data = out                                      # the old storage is unreachable only after the rebind
        release!(d.budget, oldbytes)
    end
    release!(d.budget, shellbytes(GrowBuf{T}))
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
    if g === nothing
        reserve!(d.budget, STORAGE[].vector)
        out = p.eltype === Any ? [] : Vector{p.eltype}(undef, 0)
        allocated!(d.budget, STORAGE[].vector)
        return out
    end
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
        checkcount(d, count, satadd(p.minsize, 1))
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
    if vs === nothing
        reserve!(d.budget, vectorbytes(p.eltype, 0))
        vals = Vector{p.eltype}(undef, 0)
        allocated!(d.budget, vectorbytes(p.eltype, 0))
    else
        vals = finish!(vs, d)
    end
    addinput!(d.budget, 0)
    return buildmap(p.eltype, keys, vals, d.budget)
end

function decodevalue(p::UnionPlan, d::Decoder)
    n = length(p.branches)
    n == 0 && dataerror(d, "empty union has no datum")
    i = readindex(d, n)
    if p.nullable != 0
        value = decode(p.branches[i], d)
        return i == p.nullable ? missing : value
    end
    reserve!(d.budget, unionvaluebytes())
    v = decode(p.branches[i], d)
    box = isbits(v) ? boxbytes(typeof(v)) : 0
    box > 0 && reserve!(d.budget, box)
    u = UnionValue(i, v)                                  # the shell and the box become resident here
    allocated!(d.budget, unionvaluebytes() + box)
    return u
end

function decodevalue(p::RecordPlan, d::Decoder)
    enter!(d)
    n = length(p.fields)
    reserve!(d.budget, recordbytes(n))
    vals = Vector{Any}(undef, n)
    allocated!(d.budget, vectorbytes(Any, n))
    @inbounds for i in 1:n
        v = decode(p.fields[i], d)
        b = p.boxes[i]
        b > 0 && reserve!(d.budget, b)
        vals[i] = v                                       # an isbits value boxes on assignment
        b > 0 && allocated!(d.budget, b)
    end
    leave!(d)
    r = Record(p.schema, vals, Val(:unchecked))
    allocated!(d.budget, recordbytes(n) - vectorbytes(Any, n))
    return r
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

function skipvalue(::NullPlan, d::Decoder)
    return nothing
end

function skipvalue(::BoolPlan, d::Decoder)
    return (readbool(d); nothing)
end
# Logical values are domain-checked when skipped exactly as when decoded (plan §4.3: only skipped
# strings are not UTF-8-validated).
function skipvalue(::Union{IntPlan,DatePlan}, d::Decoder)
    return (readint(d); nothing)
end

function skipvalue(p::TimeMillisPlan, d::Decoder)
    return (decodevalue(p, d); nothing)
end

function skipvalue(::Union{LongPlan,TimestampPlan,LocalTimestampPlan}, d::Decoder)
    return (readlong(d); nothing)
end

function skipvalue(p::TimeMicrosPlan, d::Decoder)
    return (decodevalue(p, d); nothing)
end

function skipvalue(::FloatPlan, d::Decoder)
    return (readfloat(d); nothing)
end

function skipvalue(::DoublePlan, d::Decoder)
    return (readdouble(d); nothing)
end

function skipvalue(::Union{BytesPlan,StringPlan}, d::Decoder)
    return skiplen(d)
end

function skipvalue(::UUIDStringPlan, d::Decoder)
    n = readlen(d, d.budget.limits.max_bytes, :max_bytes)
    validuuid(d.buf, d.pos, n) ||
        dataerror(d, diagnosticstring(d.budget, "invalid uuid string (", n, " bytes)"))
    d.pos += n
    return nothing
end

function skipvalue(p::DecimalPlan, d::Decoder)
    n = p.fixedsize == 0 ? readlen(d, d.budget.limits.max_bytes, :max_bytes) : p.fixedsize
    n == 0 && dataerror(d, "empty decimal payload")
    bytesavailable(d, n) ||
        dataerror(d, "fixed of $n bytes exceeds the remaining $(remaining(d)) bytes")
    value = decimalunscaled(d, p, d.pos, n)
    value isa BigInt && release!(d.budget, widedecimalbytes(n))
    d.pos += n
    return nothing
end

function skipvalue(p::FixedPlan, d::Decoder)
    return skipfixed(d, p.schema.size)
end

function skipvalue(::UUIDFixedPlan, d::Decoder)
    return skipfixed(d, 16)
end

function skipvalue(::DurationPlan, d::Decoder)
    return skipfixed(d, 12)
end

function skipvalue(p::EnumPlan, d::Decoder)
    return (readindex(d, length(p.schema.symbols)); nothing)
end

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
        checkcount(d, count, satadd(p.minsize, 1))
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
    return skip(p.branches[i], d)
end

function skipvalue(p::RecordPlan, d::Decoder)
    enter!(d)
    for f in p.fields
        skip(f, d)
    end
    leave!(d)
    return nothing
end

"Skip one structurally valid datum without interpreting logical-value byte order or domains."
function structuralskip(p::ReadPlan, d::Decoder)
    countvalues!(d.budget)
    return structuralskipvalue(p, d)
end

function structuralskipvalue(p::Union{NullPlan,BoolPlan,IntPlan,LongPlan,FloatPlan,
                                      DoublePlan,BytesPlan,StringPlan,FixedPlan,
                                      EnumPlan}, d::Decoder)
    return skipvalue(p, d)
end

function structuralskipvalue(::DatePlan, d::Decoder)
    return (readint(d); nothing)
end

function structuralskipvalue(::Union{TimeMillisPlan}, d::Decoder)
    return (readint(d); nothing)
end

function structuralskipvalue(::Union{TimeMicrosPlan,TimestampPlan,LocalTimestampPlan},
                             d::Decoder)
    return (readlong(d); nothing)
end

function structuralskipvalue(::UUIDStringPlan, d::Decoder)
    return skiplen(d)
end

function structuralskipvalue(p::DecimalPlan, d::Decoder)
    return p.fixedsize == 0 ? skiplen(d) : skipfixed(d, p.fixedsize)
end

function structuralskipvalue(::UUIDFixedPlan, d::Decoder)
    return skipfixed(d, 16)
end

function structuralskipvalue(::DurationPlan, d::Decoder)
    return skipfixed(d, 12)
end

function structuralskipvalue(p::ArrayPlan, d::Decoder)
    enter!(d)
    try
        while true
            count, size = readblockcount(d)
            count == 0 && break
            checkcount(d, count, p.minsize)
            outerstop = d.stop
            if size >= 0
                stop = d.pos + size - 1
                stop <= d.stop || dataerror(d, "sized block exceeds the remaining bytes")
                d.stop = stop
            end
            for _ in 1:count
                structuralskip(p.items, d)
            end
            size >= 0 &&
                (d.pos == d.stop + 1 ||
                 dataerror(d, "sized array block not exactly consumed"))
            d.stop = outerstop
        end
    finally
        leave!(d)
    end
    return nothing
end

function structuralskipvalue(p::MapPlan, d::Decoder)
    enter!(d)
    try
        while true
            count, size = readblockcount(d)
            count == 0 && break
            checkcount(d, count, satadd(p.minsize, 1))
            outerstop = d.stop
            if size >= 0
                stop = d.pos + size - 1
                stop <= d.stop || dataerror(d, "sized block exceeds the remaining bytes")
                d.stop = stop
            end
            for _ in 1:count
                skiplen(d)
                structuralskip(p.values, d)
            end
            size >= 0 &&
                (d.pos == d.stop + 1 ||
                 dataerror(d, "sized map block not exactly consumed"))
            d.stop = outerstop
        end
    finally
        leave!(d)
    end
    return nothing
end

function structuralskipvalue(p::UnionPlan, d::Decoder)
    n = length(p.branches)
    n == 0 && dataerror(d, "empty union has no datum")
    index = readindex(d, n)
    return structuralskip(p.branches[index], d)
end

function structuralskipvalue(p::RecordPlan, d::Decoder)
    enter!(d)
    try
        for field in p.fields
            structuralskip(field, d)
        end
    finally
        leave!(d)
    end
    return nothing
end
