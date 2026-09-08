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
    schema::ArraySchema
    items::WritePlan
    eltype::Type
end
struct WMap <: WritePlan
    schema::MapSchema
    values::WritePlan
    eltype::Type
end
struct WUnion <: WritePlan
    schema::UnionSchema
    branches::Vector{WritePlan}
    nullable::Int
    reprtypes::Vector{Any}           # juliatype of each branch (for exact representation matching)
end
struct WRecord <: WritePlan
    schema::RecordSchema
    fields::Vector{WritePlan}
end

function writeplan(s::Schema; budget::Union{Nothing,Budget}=nothing)
    nodes = graphinfo(s).nodes
    mbytes = vectorbytes(Union{Nothing,WritePlan}, nodes)
    checkpoint = budget === nothing ? nothing : budgetcheckpoint(budget)
    try
        budget === nothing || reserve!(budget, mbytes)     # the construction memo, released once the root is built
        memo = Vector{Union{Nothing,WritePlan}}(nothing, nodes)
        budget === nothing || allocated!(budget, mbytes)
        p = writeplan(s, memo, budget)
        budget === nothing || release!(budget, mbytes)     # the memo dies here; the plan graph stays charged
        return p
    catch
        budget === nothing || rollbackreservations!(budget, checkpoint::NTuple{2,Int})
        rethrow()
    end
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
    s.logical isa DecimalLogical &&
        return plannode(() -> WDecimal(0, s.logical.precision, s.logical.scale), budget)
    return WBytes()
end

function buildwriteplan(s::StringSchema, memo, budget)
    s.logical isa UUIDLogical && return WUUIDString()
    return WString()
end

function buildwriteplan(s::FixedSchema, memo, budget)
    l = s.logical
    l isa DecimalLogical && return plannode(() -> WDecimal(s.size, l.precision, l.scale), budget)
    l isa UUIDLogical && return WUUIDFixed()
    l isa DurationLogical && return WDuration()
    return plannode(() -> WFixed(s), budget)
end

function buildwriteplan(s::EnumSchema, memo, budget)
    return plannode(() -> WEnum(s), budget)
end

function buildwriteplan(s::ArraySchema, memo, budget)
    items = writeplan(s.items, memo, budget)
    return plannode(() -> WArray(s, items, elementtype(s.items)), budget)
end

function buildwriteplan(s::MapSchema, memo, budget)
    values = writeplan(s.values, memo, budget)
    return plannode(() -> WMap(s, values, elementtype(s.values)), budget)
end

function buildwriteplan(s::UnionSchema, memo, budget)
    n = length(s.branches)
    vectors = 2 * vectorbytes(Any, n)
    node = 64
    budget === nothing || reserve!(budget, vectors + node)
    branches = Vector{WritePlan}(undef, n)
    jtypes = Vector{Any}(undef, n)
    budget === nothing || allocated!(budget, vectors)
    for (i, b) in enumerate(s.branches)
        branches[i] = writeplan(b, memo, budget)
        jtypes[i] = juliatype(b)
    end
    p = WUnion(s, branches, nullablebranch(s), jtypes)
    budget === nothing || allocated!(budget, node)
    return p
end

function buildwriteplan(s::RecordSchema, memo, budget)
    nf = length(s.fields)
    slots = vectorbytes(WritePlan, nf) + 64
    budget === nothing || reserve!(budget, slots)
    fields = Vector{WritePlan}(undef, nf)
    resize!(fields, 0)
    p = WRecord(s, fields)
    budget === nothing || allocated!(budget, slots)
    memo[Int(nodeid(s)) + 1] = p
    for f in s.fields
        push!(p.fields, writeplan(f.schema, memo, budget))
    end
    return p
end

# ---- value paths and errors -----------------------------------------------------------------------------

function pushencodepath!(e::Encoder, kind::UInt8,
                         name::Union{Nothing,AbstractString,Symbol}, index::Int=0)
    if e.path.len == length(e.path.data)
        oldcap = length(e.path.data)
        newcap = max(checked_add(oldcap, oldcap), 4)
        oldbytes = vectorbytes(EncodePathSegment, oldcap)
        newbytes = vectorbytes(EncodePathSegment, newcap)
        active = e.budget
        owner = e.owner
        activecheckpoint = budgetcheckpoint(active)
        ownercheckpoint = active === owner ? activecheckpoint : budgetcheckpoint(owner)
        replacement = nothing
        try
            active === owner || reserve!(active, newbytes)
            reserve!(owner, newbytes)
            replacement = Vector{EncodePathSegment}(undef, newcap)
            allocated!(owner, newbytes)
            active === owner || allocated!(active, newbytes)
        catch
            replacement = nothing
            rollbackreservations!(active, activecheckpoint)
            active === owner || rollbackreservations!(owner, ownercheckpoint)
            rethrow()
        end
        copyto!(replacement, 1, e.path.data, 1, e.path.len)
        e.path.data = replacement
        release!(owner, oldbytes)
        active === owner || release!(active, oldbytes)
    end
    e.path.len += 1
    @inbounds e.path.data[e.path.len] = EncodePathSegment(kind, name, index)
    return nothing
end

function popencodepath!(e::Encoder)
    e.path.len > 0 || throw(ArgumentError("cannot pop an empty encode path"))
    e.path.data[e.path.len] = EMPTY_ENCODE_PATH_SEGMENT
    e.path.len -= 1
    return nothing
end

function diagnostickeybyte(key::AbstractString, index::Int)
    return codeunits(key)[index]
end

function diagnostickeybyte(key::Symbol, index::Int)
    pointer = Base.unsafe_convert(Ptr{UInt8}, key)
    return unsafe_load(pointer, index)
end

function encodekeyprefix(key::Union{AbstractString,Symbol})
    n = sizeof(key)
    stop = min(n, DIAGNOSTIC_NAME_PREFIX_BYTES)
    while stop < n && stop > 0 && (diagnostickeybyte(key, stop + 1) & 0xc0) == 0x80
        stop -= 1
    end
    return stop
end

function escapedkeybytes(key::Union{AbstractString,Symbol}, stop::Int)
    n = 0
    i = 1
    while i <= stop
        b = diagnostickeybyte(key, i)
        if b == UInt8('"') || b == UInt8('\\') || b in (0x08, 0x0c, 0x0a, 0x0d, 0x09)
            n += 2; i += 1
        elseif b < 0x20
            n += 6; i += 1
        elseif b == 0xed && i + 2 <= stop &&
               diagnostickeybyte(key, i + 1) >= 0xa0 &&
               (diagnostickeybyte(key, i + 1) & 0xc0) == 0x80 &&
               (diagnostickeybyte(key, i + 2) & 0xc0) == 0x80
            n += 6; i += 3
        else
            n += 1; i += 1
        end
    end
    return n + (stop < sizeof(key) ? 3 : 0)
end

function writehex4!(buf::Vector{UInt8}, pos::Int, value::UInt32)
    buf[pos] = UInt8('\\'); buf[pos + 1] = UInt8('u')
    for i in 0:3
        shift = 4 * (3 - i)
        buf[pos + 2 + i] = HEX_DIGITS[Int((value >> shift) & 0x0f) + 1]
    end
    return pos + 6
end

function writeescapedkey!(buf::Vector{UInt8}, pos::Int,
                          key::Union{AbstractString,Symbol}, stop::Int)
    i = 1
    while i <= stop
        b = diagnostickeybyte(key, i)
        if b == UInt8('"') || b == UInt8('\\')
            buf[pos] = UInt8('\\'); buf[pos + 1] = b; pos += 2; i += 1
        elseif b in (0x08, 0x0c, 0x0a, 0x0d, 0x09)
            buf[pos] = UInt8('\\')
            buf[pos + 1] = b == 0x08 ? UInt8('b') : b == 0x0c ? UInt8('f') :
                           b == 0x0a ? UInt8('n') : b == 0x0d ? UInt8('r') : UInt8('t')
            pos += 2; i += 1
        elseif b < 0x20
            pos = writehex4!(buf, pos, UInt32(b)); i += 1
        elseif b == 0xed && i + 2 <= stop &&
               diagnostickeybyte(key, i + 1) >= 0xa0 &&
               (diagnostickeybyte(key, i + 1) & 0xc0) == 0x80 &&
               (diagnostickeybyte(key, i + 2) & 0xc0) == 0x80
            cp = (UInt32(b & 0x0f) << 12) |
                 (UInt32(diagnostickeybyte(key, i + 1) & 0x3f) << 6) |
                 UInt32(diagnostickeybyte(key, i + 2) & 0x3f)
            pos = writehex4!(buf, pos, cp); i += 3
        else
            buf[pos] = b; pos += 1; i += 1
        end
    end
    if stop < sizeof(key)
        buf[pos] = 0xe2; buf[pos + 1] = 0x80; buf[pos + 2] = 0xa6
        pos += 3
    end
    return pos
end

function encodepathbytes(e::Encoder)
    n = 1
    for i in 1:e.path.len
        segment = e.path.data[i]
        if segment.kind == ENCODE_PATH_FIELD
            n = checked_add(n, checked_add(1, sizeof(segment.name::String)))
        elseif segment.kind == ENCODE_PATH_INDEX
            n = checked_add(n, checked_add(2, integerdigits(segment.index)))
        else
            key = segment.name::Union{AbstractString,Symbol}
            stop = encodekeyprefix(key)
            n = checked_add(n, checked_add(4, escapedkeybytes(key, stop)))
        end
    end
    return n
end

function formatencodepath(e::Encoder)
    n = encodepathbytes(e)
    buf = diagnosticbuffer(n, e.budget)
    pos = 1
    buf[pos] = UInt8('$'); pos += 1
    for i in 1:e.path.len
        segment = e.path.data[i]
        if segment.kind == ENCODE_PATH_FIELD
            buf[pos] = UInt8('.')
            pos = writediagnosticpart!(buf, pos + 1, segment.name::String)
        elseif segment.kind == ENCODE_PATH_INDEX
            buf[pos] = UInt8('[')
            pos = writediagnosticpart!(buf, pos + 1, segment.index)
            buf[pos] = UInt8(']'); pos += 1
        else
            key = segment.name::Union{AbstractString,Symbol}
            stop = encodekeyprefix(key)
            buf[pos] = UInt8('['); buf[pos + 1] = UInt8('"')
            pos = writeescapedkey!(buf, pos + 2, key, stop)
            buf[pos] = UInt8('"'); buf[pos + 1] = UInt8(']'); pos += 2
        end
    end
    pos == n + 1 || throw(ArgumentError("encode path size mismatch"))
    return finishdiagnostic!(buf, e.budget)
end

function encodeerror(msg::AbstractString, x)
    throw(EncodeError(string(msg, " (got ", nameof(typeof(x)), ")"), "", nothing))
end

# ---- encoding --------------------------------------------------------------------------------------------

"""
    encode(plan, e::Encoder, x)

Encode `x` under `plan` into `e`, validating it against the schema and charging values to the budget.
"""
function encode(p::WritePlan, e::Encoder, x)
    b = e.budget
    if b.workdeferred == 0 && b.values >= b.workcap && e.pos > e.credited
        creditoutput!(e)
    end
    countvalues!(b)
    try
        encodevalue(p, e, x)
    catch err
        if err isa EncodeError && isempty(err.path)
            throw(EncodeError(err.msg, formatencodepath(e), e.expected))
        end
        rethrow()
    end
    return nothing
end

function encodechild(p::WritePlan, e::Encoder, @nospecialize(x), schema::Schema,
                     kind::UInt8,
                     name::Union{Nothing,AbstractString,Symbol}=nothing,
                     index::Int=0)
    previous = e.expected
    pushencodepath!(e, kind, name, index)
    e.expected = schema
    try
        return encode(p, e, x)
    catch err
        if err isa EncodeError && isempty(err.path)
            throw(EncodeError(err.msg, formatencodepath(e), e.expected))
        end
        rethrow()
    finally
        e.expected = previous
        popencodepath!(e)
    end
end

function withencodepath!(f::F, e::Encoder, schema::Schema, kind::UInt8,
                         name::Union{Nothing,AbstractString,Symbol}=nothing,
                         index::Int=0) where {F}
    previous = e.expected
    pushencodepath!(e, kind, name, index)
    e.expected = schema
    try
        return f()
    catch err
        if err isa EncodeError && isempty(err.path)
            throw(EncodeError(err.msg, formatencodepath(e), e.expected))
        end
        rethrow()
    finally
        e.expected = previous
        popencodepath!(e)
    end
end

function encodebranch(p::WritePlan, e::Encoder, @nospecialize(x), schema::Schema)
    previous = e.expected
    e.expected = schema
    try
        return encode(p, e, x)
    catch err
        if err isa EncodeError && isempty(err.path)
            throw(EncodeError(err.msg, formatencodepath(e), e.expected))
        end
        rethrow()
    finally
        e.expected = previous
    end
end

function withencodeschema!(f::F, e::Encoder, schema::Schema) where {F}
    previous = e.expected
    e.expected = schema
    try
        return f()
    catch err
        if err isa EncodeError && isempty(err.path)
            throw(EncodeError(err.msg, formatencodepath(e), e.expected))
        end
        rethrow()
    finally
        e.expected = previous
    end
end

"Credit every newly produced byte to the encode-side work and comparison denominator."
function creditoutput!(e::Encoder)
    n = e.pos - e.credited
    n > 0 || return nothing
    addinput!(e.budget, n)
    e.credited = e.pos
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
    typemin(Int32) <= x <= typemax(Int32) || encodeerror("integer does not fit an Avro int", x)
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
    typemin(Int64) <= x <= typemax(Int64) || encodeerror("integer does not fit an Avro long", x)
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
    isstrictutf8(x) || encodeerror("string is not valid UTF-8", x)
    writestring!(e, x)
    return nothing
end

function encodevalue(::WString, e::Encoder, x::Symbol)
    isstrictutf8(x) || encodeerror("string is not valid UTF-8", x)
    return writesymbol!(e, x)
end

function encodevalue(::WString, e::Encoder, x::Char)
    isvalid(x) || encodeerror("string is not valid UTF-8", x)
    return writechar!(e, x)
end

function encodevalue(::WString, e::Encoder, x)
    return encodeerror("expected a string", x)
end

function encodevalue(p::WFixed, e::Encoder, x::Fixed)
    (fullnameequal(x.schema.name, p.schema.name) && x.schema.size == p.schema.size) || encodeerror("fixed value does not match the writer schema", x)
    length(x.bytes) == p.schema.size || encodeerror("fixed $(fullname(p.schema)) needs exactly $(p.schema.size) bytes, got $(length(x.bytes))", x)
    checkvaluebytes(e.budget, p.schema.size)
    writeraw!(e, x.bytes)
    return nothing
end

function encodevalue(p::WFixed, e::Encoder, x::AbstractVector{UInt8})
    length(x) == p.schema.size || encodeerror("fixed $(fullname(p.schema)) needs exactly $(p.schema.size) bytes, got $(length(x))", x)
    checkvaluebytes(e.budget, p.schema.size)
    writeraw!(e, x)
    return nothing
end

function encodevalue(p::WFixed, e::Encoder, x::NTuple{N,UInt8}) where {N}
    N == p.schema.size || encodeerror("fixed $(fullname(p.schema)) needs exactly $(p.schema.size) bytes, got $N", x)
    checkvaluebytes(e.budget, N)
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
    if x.schema === p.schema || (fullnameequal(x.schema.name, p.schema.name) && x.schema.symbols.data == p.schema.symbols.data)
        writelong!(e, Int64(x.index) - 1)
        return nothing
    end
    return encodesymbol(p, e, String(x))
end

function encodevalue(p::WEnum, e::Encoder, x::AbstractString)
    return encodesymbol(p, e, x)
end

function encodevalue(p::WEnum, e::Encoder, x::Symbol)
    i = enumindex(p, x, e.budget)
    i == 0 && encodeerror("the symbol is not a member of enum $(fullname(p.schema))", x)
    return writelong!(e, Int64(i) - 1)
end

function encodevalue(p::WEnum, e::Encoder, x::Base.Enum)
    return encodesymbol(p, e, avrosymbol(typeof(x), x))
end

function encodevalue(p::WEnum, e::Encoder, x)
    return encodeerror("expected an enum symbol of $(fullname(p.schema))", x)
end

function encodesymbol(p::WEnum, e::Encoder, sym::AbstractString)
    i = budgetedget(p.schema.symbolindex, sym, 0, e.budget)
    i == 0 && encodeerror(diagnosticstring(e.budget,
                                           "the value is not a symbol of enum ",
                                           p.schema.name), sym)
    writelong!(e, Int64(i) - 1)
    return nothing
end

function enumindex(p::WEnum, symbol::Symbol, budget::Union{Nothing,Budget})
    for (i, candidate) in enumerate(p.schema.symbols)
        equal, work = symbolcomparison(candidate, symbol)
        budget === nothing || addcompare!(budget, work)
        equal && return i
    end
    return 0
end

function symbolcomparison(candidate::String, symbol::Symbol)
    n = sizeof(candidate)
    n == sizeof(symbol) || return (false, min(n, sizeof(symbol)) + 1)
    source = Base.unsafe_convert(Ptr{UInt8}, symbol)
    GC.@preserve candidate symbol begin
        for i in 1:n
            codeunit(candidate, i) == unsafe_load(source, i) || return (false, i)
        end
    end
    return (true, n + 1)
end

function encodevalue(::WDate, e::Encoder, x::Date)
    days = Dates.value(x - DATE_EPOCH)
    typemin(Int32) <= days <= typemax(Int32) ||
        return encodeerror("date is outside the Avro int range", x)
    return writeint!(e, Int32(days))
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
    return writetwoscomplement!(e, p, x.unscaled)
end

function encodevalue(p::WDecimal, e::Encoder, x::WideDecimal)
    x.scale == p.scale || encodeerror("decimal scale $(x.scale) does not equal the schema scale $(p.scale) (rescale first)", x)
    ndigits(x.unscaled) <= p.precision || encodeerror("decimal exceeds precision $(p.precision)", x)
    return writetwoscomplement!(e, p, x.unscaled)
end

function encodevalue(p::WDecimal, e::Encoder, x)
    return encodeerror("expected an Avro.Decimal/WideDecimal with scale $(p.scale)", x)
end

"""
    twoscomplement(v, budget; maxbytes=budget.limits.max_bytes) -> Vector{UInt8}

The minimal big-endian two's complement representation of `v`. The returned vector remains charged
to `budget`; its caller releases `bytesbytes(length(result))` after its last use.
"""
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

function twoscomplementlength(v::BigInt)
    bits = Base.GMP.MPZ.sizeinbase(v, 2)
    v >= 0 && return max(1, cld(bits + 1, 8))
    complementbits = Base.GMP.MPZ.mpn_popcount(v) == 1 ? bits - 1 : bits
    return max(1, cld(complementbits + 1, 8))
end

function filltwoscomplement!(out::Vector{UInt8}, v::Int128)
    bits = reinterpret(UInt128, v)
    for i in 0:length(out) - 1
        @inbounds out[end - i] = UInt8((bits >> (8 * i)) & 0xff)
    end
    return out
end

function filltwoscomplement!(out::Vector{UInt8}, v::BigInt)
    fill!(out, 0x00)
    bits = Base.GMP.MPZ.sizeinbase(v, 2)
    magnitude = iszero(v) ? 0 : cld(bits, 8)
    if magnitude > 0
        GC.@preserve out v ccall((:__gmpz_export, Base.GMP.libgmp), Ptr{UInt8},
            (Ptr{UInt8}, Ptr{Csize_t}, Cint, Csize_t, Cint, Csize_t, Ref{BigInt}),
            pointer(out, length(out) - magnitude + 1), C_NULL, 1, 1, 1, 0, v)
    end
    if v < 0
        carry = true
        for i in eachindex(out)
            @inbounds out[i] = ~out[i]
        end
        for i in length(out):-1:1
            carry || break
            @inbounds value, carry = Base.add_with_overflow(out[i], UInt8(1))
            @inbounds out[i] = value
        end
    end
    return out
end

function twoscomplement(v::Union{Int128,BigInt}, budget::Budget; maxbytes::Int=budget.limits.max_bytes)
    n = twoscomplementlength(v)
    n <= maxbytes || throw(limiterror(budget, :max_bytes, n, maxbytes))
    charge = bytesbytes(n)
    checkpoint = budgetcheckpoint(budget)
    try
        reserve!(budget, charge)
        out = Vector{UInt8}(undef, n)
        allocated!(budget, charge)
        return filltwoscomplement!(out, v)
    catch
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

function writetwoscomplement!(e::Encoder, p::WDecimal, v::Union{Int128,BigInt})
    fixed = p.fixedsize
    fixed == 0 || checkvaluebytes(e.budget, fixed)
    n = twoscomplementlength(v)
    fixed == 0 || n <= fixed || encodeerror("decimal does not fit a fixed of $fixed bytes", v)
    bytes = twoscomplement(v, e.budget; maxbytes=fixed == 0 ? e.budget.limits.max_bytes : fixed)
    try
        if fixed == 0
            writebytes!(e, bytes)
            return nothing
        end
        pad = v < 0 ? 0xff : 0x00
        ensureroom!(e, fixed)
        for _ in 1:fixed - length(bytes)
            writebyte!(e, pad)
        end
        writeraw!(e, bytes)
    finally
        release!(e.budget, bytesbytes(length(bytes)))
    end
    return nothing
end

"Write the canonical lowercase UUID text directly into an encoder."
function writeuuidstring!(e::Encoder, u::UUID)
    n = 36
    n <= e.budget.limits.max_bytes ||
        throw(LimitError(:max_bytes, n, e.budget.limits.max_bytes, :max_bytes, :encode))
    writelong!(e, Int64(n))
    ensureroom!(e, n)
    value = UInt128(u)
    pos = e.pos
    @inbounds for i in 0:31
        if i == 8 || i == 12 || i == 16 || i == 20
            pos += 1
            e.buf[pos] = UInt8('-')
        end
        pos += 1
        e.buf[pos] = HEX_DIGITS[Int((value >> (4 * (31 - i))) & 0x0f) + 1]
    end
    e.pos = pos
    return nothing
end

"Write the 16 big-endian UUID bytes directly into an encoder."
function writeuuidfixed!(e::Encoder, u::UUID)
    checkvaluebytes(e.budget, 16)
    ensureroom!(e, 16)
    value = UInt128(u)
    pos = e.pos
    @inbounds for i in 0:15
        e.buf[pos + i + 1] = UInt8((value >> (8 * (15 - i))) & 0xff)
    end
    e.pos = pos + 16
    return nothing
end

function encodevalue(::WUUIDString, e::Encoder, x::UUID)
    return writeuuidstring!(e, x)
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
    return writeuuidfixed!(e, x)
end

function encodevalue(::WUUIDFixed, e::Encoder, x)
    return encodeerror("expected a UUID", x)
end

function encodevalue(::WDuration, e::Encoder, x::Duration)
    checkvaluebytes(e.budget, 12)
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

# arrays: one positive-count block for sized inputs; bounded one-item blocks for unknown-size streams
function encodevalue(p::WArray, e::Encoder, x)
    items = arrayitems(x)
    enter!(e)
    if Base.IteratorSize(typeof(items)) isa Union{Base.HasLength,Base.HasShape}
        n = length(items)
        checkblockcount(e, n)
        n > 0 && writelong!(e, Int64(n))
        actual = 0
        for v in items
            actual += 1
            actual <= n || encodeerror("array iterator produced more than its declared length $n", x)
            encodechild(p.items, e, v, p.schema.items, ENCODE_PATH_INDEX, nothing,
                        actual - 1)
        end
        actual == n || encodeerror("array iterator produced $actual values but declared length $n", x)
    else
        checkblockcount(e, 1)
        index = 0
        for v in items
            writelong!(e, Int64(1))
            encodechild(p.items, e, v, p.schema.items, ENCODE_PATH_INDEX, nothing,
                        index)
            index += 1
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
    applicable(iterate, x) && return x
    return encodeerror("expected an array (an iterable collection)", x)
end

function encodevalue(p::WMap, e::Encoder, x)
    pairs = mappairs(x)
    enter!(e)
    n = length(pairs)
    checkblockcount(e, n)
    if n > 0
        keys, values, perm, scratch, charge = collectmapentries(pairs, e.budget)
        try
            writelong!(e, Int64(n))
            for i in 1:n
                writestring!(e, keys[i])
                encodechild(p.values, e, values[i], p.schema.values,
                            ENCODE_PATH_KEY, keys[i])
            end
            creditoutput!(e)                           # comparisons use the exact produced-byte denominator
            addcompare!(e.budget, checked_mul(16, n))  # collected key and value references
            mergesort!(perm, scratch, keys, e.budget)
            for i in 2:n
                a = keys[perm[i - 1]]
                b = keys[perm[i]]
                keyequal(a, b, e.budget) && encodeerror("map keys are duplicates after conversion to strings", pairs)
            end
        finally
            keys = values = perm = scratch = nothing
            release!(e.budget, charge)
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

"Collect a known-size map into exact, charged scratch used for encoding and duplicate detection."
function collectmapentries(pairs, budget::Budget)
    n = length(pairs)
    vectors = vectorbytes(String, n) + vectorbytes(Any, n) +
              vectorbytes(Int32, n) + vectorbytes(Int32, cld(n, 2))
    checkpoint = budgetcheckpoint(budget)
    keys = values = perm = scratch = nothing
    strings = boxes = 0
    try
        reserve!(budget, vectors)
        keys = Vector{String}(undef, n)
        values = Vector{Any}(undef, n)
        perm = Vector{Int32}(undef, n)
        scratch = Vector{Int32}(undef, cld(n, 2))
        allocated!(budget, vectors)
        i = 0
        for (k, v) in pairs
            i < n || encodeerror("map size changed during iteration", pairs)
            i += 1
            key = copymapkey(k, budget)
            strings = checked_add(strings, stringbytes(sizeof(key)))
            keys[i] = key
            box = isbits(v) ? boxbytes(typeof(v)) : 0
            box > 0 && reserve!(budget, box)
            values[i] = v
            box > 0 && allocated!(budget, box)
            boxes = checked_add(boxes, box)
        end
        i == n || encodeerror("map size changed during iteration", pairs)
        return keys, values, perm, scratch, checked_add(checked_add(vectors, strings), boxes)
    catch
        keys = values = perm = scratch = nothing
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

# unions: branch recovery (plan §4.6)
function encodevalue(p::WUnion, e::Encoder, x)
    i = selectbranch(p, x, e.budget)
    writelong!(e, Int64(i) - 1)
    encodebranch(p.branches[i], e, x isa UnionValue ? x.value : x,
                 p.schema.branches[i])
    return nothing
end

function encodevalue(p::WUnion, e::Encoder, x::Base.Enum)
    i, symbol = selectenumbranch(p, x, e.budget)
    writelong!(e, Int64(i) - 1)
    countvalues!(e.budget)
    encodesymbol(p.branches[i]::WEnum, e, symbol)
    return nothing
end

function selectbranch(p::WUnion, x, budget::Union{Nothing,Budget}=nothing)
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
        s = retainedidentityschema(x)
        for (i, b) in enumerate(p.schema.branches)
            b isa NamedSchema || continue
            typeof(b) === typeof(s) || continue
            budgetedfullnameequal(b.name, s.name, budget) || continue
            b isa FixedSchema && b.size != (s::FixedSchema).size && continue
            return i
        end
        encodeerror("no union branch has the identity $(fullname(s))", x)
    end
    T = typeof(x)
    for (i, r) in enumerate(p.reprtypes)
        r === T && return i
    end
    x isa Base.Enum && return first(selectenumbranch(p, x, budget))
    for (i, b) in enumerate(p.branches)
        acceptsbranch(b, x, budget) && return i
    end
    encodeerror("no union branch accepts the value", x)
end

function retainedidentityschema(x::Record)
    return getfield(x, :schema)
end

function retainedidentityschema(x::Union{EnumValue,Fixed})
    return x.schema
end

function selectenumbranch(p::WUnion, x::Base.Enum, budget::Union{Nothing,Budget})
    symbol = avrosymbol(typeof(x), x)
    symbol isa AbstractString || encodeerror("Avro.avrosymbol must return a string", x)
    for (i, branch) in enumerate(p.branches)
        branch isa WEnum || continue
        budgetedhaskey(branch.schema.symbolindex, symbol, budget) && return (i, symbol)
    end
    encodeerror("no union enum branch accepts the symbol", x)
end

function acceptsbranch(p::WritePlan, x, budget::Union{Nothing,Budget})
    return accepts(p, x)
end

function acceptsbranch(p::WEnum, x::AbstractString, budget::Union{Nothing,Budget})
    return budgetedhaskey(p.schema.symbolindex, x, budget)
end

function acceptsbranch(p::WEnum, x::Symbol, budget::Union{Nothing,Budget})
    return enumindex(p, x, budget) != 0
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
    return (x isa Fixed && fullnameequal(x.schema.name, p.schema.name)) || ((x isa AbstractVector{UInt8} || x isa NTuple{N,UInt8} where {N}) && length(x) == p.schema.size)
end

function accepts(p::WEnum, x)
    return (x isa EnumValue && fullnameequal(x.schema.name, p.schema.name)) ||
           (x isa AbstractString && budgetedhaskey(p.schema.symbolindex, x, nothing)) ||
           (x isa Symbol && enumindex(p, x, nothing) != 0)
end

function accepts(p::WEnum, x::Base.Enum)
    symbol = avrosymbol(typeof(x), x)
    return symbol isa AbstractString && budgetedhaskey(p.schema.symbolindex, symbol, nothing)
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
    return (x isa DataDecimals.AbstractDecimal ? DataDecimals.scale(x) == p.scale : x isa WideDecimal && x.scale == p.scale)
end

function accepts(::WUUIDString, x)
    return x isa UUID || (x isa AbstractString && tryparseuuid(x) !== nothing)
end

function accepts(::WUUIDFixed, x)
    return x isa UUID
end

function accepts(::WDuration, x)
    return x isa Union{Duration,Durations.Duration}
end

function accepts(::WArray, x)
    return !(x isa Union{AbstractString,AbstractDict}) && applicable(iterate, x)
end

function accepts(::WMap, x)
    return x isa Map || x isa AbstractDict || x isa NamedTuple
end

function accepts(::WUnion, x)
    return false
end

function accepts(p::WRecord, x)
    return x isa Record ? fullnameequal(getfield(x, :schema).name, p.schema.name) : (x isa NamedTuple || x isa AbstractDict || isrecordlike(x))
end

# Plain structs are record-like unless they are one of the scalar/container kinds the other branches own.
const NOT_RECORDLIKE = Union{Missing,Nothing,Number,AbstractString,Symbol,Char,AbstractArray,Tuple,AbstractSet,Type,Function,
                             Date,Time,DateTime,UUID,Decimal,WideDecimal,Timestamp,LocalTimestamp,Duration,Durations.Duration,Fixed,EnumValue,Map,UnionValue,Base.Enum}
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
    aligned = xs === p.schema
    if !aligned && length(xs.fields) == length(p.schema.fields)
        aligned = budgetedfullnameequal(xs.name, p.schema.name, e.budget)
        for i in eachindex(p.fields)
            aligned || break
            aligned = keyequal(xs.fields[i].name, p.schema.fields[i].name, e.budget)
        end
    end
    if aligned
        for (i, f) in enumerate(p.fields)
            field = p.schema.fields[i]
            encodechild(f, e, vals[i], field.schema, ENCODE_PATH_FIELD, field.name)
        end
        return nothing
    end
    for (i, f) in enumerate(p.schema.fields)
        j = budgetedget(xs.fieldindex, f.name, 0, e.budget)
        j == 0 && encodeerror("record value lacks field \"$(f.name)\"", x)
        encodechild(p.fields[i], e, vals[j], f.schema, ENCODE_PATH_FIELD, f.name)
    end
    return nothing
end

function encoderecord(p::WRecord, e::Encoder, x::AbstractDict)
    values, boxes = recorddictvalues(p, x, e.budget)
    try
        for (i, v) in enumerate(values)
            v === DICT_MISSING && encodeerror("record value lacks field \"$(p.schema.fields[i].name)\"", x)
            field = p.schema.fields[i]
            encodechild(p.fields[i], e, v, field.schema, ENCODE_PATH_FIELD, field.name)
        end
    finally
        release!(e.budget, vectorbytes(Any, length(values)) + boxes)
    end
    return nothing
end

const DICT_MISSING = Val(:missing_field)

function dictfield(x::AbstractDict, name::String, budget::Union{Nothing,Budget}=nothing)
    for (k, v) in x
        (k isa Symbol || k isa AbstractString) ||
            encodeerror("record dictionary keys must be strings or symbols", k)
        budget === nothing || addcompare!(budget, min(sizeof(name), sizeof(k)) + 1)
        fieldnamematches(name, k) && return v
    end
    return DICT_MISSING
end

function encoderecord(p::WRecord, e::Encoder, x::T) where {T}
    if x isa Tables.AbstractRow
        positions = recordrowpositions(p, x, e.budget)
        try
            for i in eachindex(p.fields)
                j = positions[i]
                j == 0 && encodeerror("row lacks column \"$(p.schema.fields[i].name)\"", x)
                field = p.schema.fields[i]
                encodechild(p.fields[i], e, Tables.getcolumn(x, j), field.schema,
                            ENCODE_PATH_FIELD, field.name)
            end
        finally
            release!(e.budget, vectorbytes(Int, length(positions)))
        end
        return nothing
    end
    (x isa NamedTuple || (isstructtype(T) && !(x isa AbstractArray) && !(x isa AbstractString))) || encodeerror("expected a record value for $(fullname(p.schema))", x)
    positions = recordfieldpositions(p, T, e.budget)
    try
        for (i, f) in enumerate(p.fields)
            j = positions[i]
            j == 0 && encodeerror("$(T) has no field for \"$(p.schema.fields[i].name)\" of record $(fullname(p.schema))", x)
            field = p.schema.fields[i]
            encodechild(f, e, getfield(x, j), field.schema, ENCODE_PATH_FIELD,
                        field.name)
        end
    finally
        release!(e.budget, vectorbytes(Int, length(positions)))
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
    positions = recordrowpositions(p, x.row, e.budget)
    try
        for i in eachindex(p.fields)
            j = positions[i]
            j == 0 && encodeerror("row lacks column \"$(p.schema.fields[i].name)\"", x.row)
            field = p.schema.fields[i]
            encodechild(p.fields[i], e, Tables.getcolumn(x.row, j), field.schema,
                        ENCODE_PATH_FIELD, field.name)
        end
    finally
        release!(e.budget, vectorbytes(Int, length(positions)))
    end
    return nothing
end

function rowfield(row, name::String, budget::Union{Nothing,Budget}=nothing)
    for (i, c) in enumerate(Tables.columnnames(row))
        (c isa Symbol || c isa AbstractString) ||
            encodeerror("row column names must be strings or symbols", c)
        budget === nothing || addcompare!(budget, min(sizeof(name), sizeof(c)) + 1)
        fieldnamematches(name, c) &&
            return Tables.getcolumn(row, i)
    end
    encodeerror("row lacks column \"$name\"", row)
end

"Allocate and zero one exact schema-field position vector under the operation budget."
function recordpositions(n::Int, budget::Budget)
    charge = vectorbytes(Int, n)
    checkpoint = budgetcheckpoint(budget)
    try
        reserve!(budget, charge)
        positions = Vector{Int}(undef, n)
        allocated!(budget, charge)
        fill!(positions, 0)
        return positions
    catch
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

function recordfieldindex(p::WRecord, candidate::String, budget::Budget)
    return budgetedget(p.schema.fieldindex, candidate, 0, budget)
end

function recordfieldindex(p::WRecord, candidate::Union{Symbol,AbstractString}, budget::Budget)
    charge = stringbytes(sizeof(candidate))
    reserve!(budget, charge)
    name = try
        value = String(candidate)
        allocated!(budget, charge)
        value
    catch
        unreserve!(budget, charge)
        rethrow()
    end
    try
        return recordfieldindex(p, name, budget)
    finally
        name = nothing
        release!(budget, charge)
    end
end

function recordfieldpositions(p::WRecord, ::Type{T}, budget::Budget) where {T}
    positions = recordpositions(length(p.fields), budget)
    isempty(p.fields) && return positions
    try
        tags = T <: NamedTuple ? (;) : StructUtils.fieldtags(AvroStyle(), T)
        for j in 1:fieldcount(T)
            fname = fieldname(T, j)
            tagged = T <: NamedTuple ? nothing : fieldtag(tags, fname, :name)
            candidate = tagged === nothing ? fname : tagged
            (candidate isa Symbol || candidate isa AbstractString) ||
                throw(ArgumentError("StructUtils field tag `name` must be a Symbol or string, got $(typeof(candidate))"))
            i = recordfieldindex(p, candidate, budget)
            i == 0 || positions[i] != 0 || (positions[i] = j)
        end
        return positions
    catch
        positions = nothing
        release!(budget, vectorbytes(Int, length(p.fields)))
        rethrow()
    end
end

function recordrowpositions(p::WRecord, row, budget::Budget)
    positions = recordpositions(length(p.fields), budget)
    isempty(p.fields) && return positions
    try
        for (j, candidate) in enumerate(Tables.columnnames(row))
            (candidate isa Symbol || candidate isa AbstractString) ||
                encodeerror("row column names must be strings or symbols", candidate)
            i = recordfieldindex(p, candidate, budget)
            i == 0 || positions[i] != 0 || (positions[i] = j)
        end
        return positions
    catch
        positions = nothing
        release!(budget, vectorbytes(Int, length(p.fields)))
        rethrow()
    end
end

function recorddictvalues(p::WRecord, x::AbstractDict, budget::Budget)
    charge = vectorbytes(Any, length(p.fields))
    checkpoint = budgetcheckpoint(budget)
    values = try
        reserve!(budget, charge)
        result = Vector{Any}(undef, length(p.fields))
        allocated!(budget, charge)
        fill!(result, DICT_MISSING)
        result
    catch
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
    boxes = 0
    isempty(p.fields) && return values, boxes
    try
        for (candidate, value) in x
            (candidate isa Symbol || candidate isa AbstractString) ||
                encodeerror("record dictionary keys must be strings or symbols", candidate)
            i = recordfieldindex(p, candidate, budget)
            (i == 0 || values[i] !== DICT_MISSING) && continue
            box = isbits(value) ? boxbytes(typeof(value)) : 0
            box > 0 && reserve!(budget, box)
            values[i] = value
            box > 0 && allocated!(budget, box)
            boxes = checked_add(boxes, box)
        end
        return values, boxes
    catch
        values = nothing
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

"""
    fieldposition(plan, T, i) -> Int

The position of the Julia field of `T` matching Avro field `i`, or zero when none exists. Name lookup
does not intern schema names or retain source types. A stateless lookup also keeps recursive records
correct when one schema node is encoded from different Julia layouts at different depths.
"""
function fieldposition(p::WRecord, ::Type{T}, i::Int,
                       budget::Union{Nothing,Budget}=nothing) where {T}
    f = p.schema.fields[i]
    tags = T <: NamedTuple ? (;) : StructUtils.fieldtags(AvroStyle(), T)
    for (j, fname) in enumerate(fieldnames(T))
        tagged = T <: NamedTuple ? nothing : fieldtag(tags, fname, :name)
        candidate = tagged === nothing ? fname : tagged
        if budget !== nothing && (candidate isa Symbol || candidate isa AbstractString)
            addcompare!(budget, min(sizeof(f.name), sizeof(candidate)) + 1)
        end
        fieldnamematches(f.name, candidate) && return j
    end
    return 0
end

function fieldnamematches(name::String, candidate::Symbol)
    n = sizeof(name)
    n == sizeof(candidate) || return false
    ptr = Base.unsafe_convert(Ptr{UInt8}, candidate)
    GC.@preserve name candidate begin
        for i in 1:n
            codeunit(name, i) == unsafe_load(ptr, i) || return false
        end
    end
    return true
end

function fieldnamematches(name::String, candidate::AbstractString)
    return name == candidate
end

function fieldnamematches(name::String, candidate)
    throw(ArgumentError("StructUtils field tag `name` must be a Symbol or string, got $(typeof(candidate))"))
end

# ---- the aligned-NamedTuple fast path (Phase 4d performance work) ------------------------------------
#
# `Avro.write` streams Tables rows, most commonly NamedTuples whose fields align 1:1 with the record
# plan. A per-writer cache holds the plan fields as a concrete tuple; the estimate and encode walks then
# run the *same* per-plan functions monomorphized through tuple recursion — identical arithmetic and
# validation, without per-field dynamic dispatch or boxing.

function alignedtuplebytes(fields::Vector{WritePlan})
    bytes = 0
    for field in fields
        bytes = checked_add(bytes, sizeof(field))
    end
    return bytes
end

"The charged plan-field tuple for `T`, or `nothing` when `T` is not an aligned NamedTuple."
function alignedplans(p::WritePlan, ::Type{T}, budget::Union{Nothing,Budget}=nothing) where {T}
    p isa WRecord || return nothing
    T <: NamedTuple || return nothing
    n = length(p.fields)
    (0 < n <= 32 && fieldcount(T) == n) || return nothing
    for i in 1:n
        fieldposition(p, T, i) == i || return nothing
    end
    charge = alignedtuplebytes(p.fields)
    budget === nothing || reserve!(budget, charge)
    plans = try
        Tuple(p.fields)
    catch
        budget === nothing || unreserve!(budget, charge)
        rethrow()
    end
    sizeof(plans) == charge || throw(ArgumentError("internal error: aligned-plan tuple layout changed"))
    budget === nothing || allocated!(budget, charge)
    return plans
end

@inline function estfields(::Tuple{}, ::Tuple{}, fields,
                           cols::Union{Nothing,Vector{Type}}, i::Int,
                           budget::Budget,
                           diagnostics::Union{Nothing,Encoder})
    return (0, 0, 0, 0)
end

@inline function estfields(plans::Tuple, vals::Tuple, fields,
                           cols::Union{Nothing,Vector{Type}}, i::Int,
                           budget::Budget,
                           diagnostics::Union{Nothing,Encoder})
    plan = first(plans)
    value = first(vals)
    estimate = if diagnostics === nothing
        estimatevalue(plan, value, budget)
    else
        field = fields[i]
        withencodepath!(diagnostics, field.schema, ENCODE_PATH_FIELD,
                        field.name) do
            estimatevalue(plan, value, budget, diagnostics)
        end
    end
    retained = checked_add(estimate.retained, estimatedbox(plan, value))
    payload = cols === nothing ? 0 : estimatedelement(cols[i], estimate, plan, value)
    restretained, restpeak, restvalues, restpayload =
        estfields(Base.tail(plans), Base.tail(vals), fields, cols, i + 1,
                  budget, diagnostics)
    return (checked_add(retained, restretained), max(estimate.peakextra, restpeak),
            checkedvalueadd(budget, estimate.values, restvalues),
            checked_add(payload, restpayload))
end

"The §4.9 estimate of one aligned row (the `estimaterootrecord` arithmetic, monomorphized)."
function estimatealigned(plans::Tuple, x::NamedTuple,
                         cols::Union{Nothing,Vector{Type}}, budget::Budget,
                         fields=nothing,
                         diagnostics::Union{Nothing,Encoder}=nothing)
    schemafields = fields === nothing ? () : fields
    retained, peakextra, nvalues, payload =
        diagnostics === nothing ?
        estfields(plans, values(x), schemafields, cols, 1, budget, nothing) :
        estfields(plans, values(x), schemafields, cols, 1, budget,
                  diagnostics)
    root = checked_add(representationslot(Record), checked_add(recordbytes(length(plans)), retained))
    return (root, peakextra, 1 + nvalues, payload)
end

@inline function encfields(e::Encoder, ::Tuple{}, ::Tuple{}, fields, i::Int)
    return nothing
end

@inline function encfieldsfast(e::Encoder, ::Tuple{}, ::Tuple{})
    return nothing
end

@inline function encfieldsfast(e::Encoder, plans::Tuple, vals::Tuple)
    encode(first(plans), e, first(vals))
    return encfieldsfast(e, Base.tail(plans), Base.tail(vals))
end

"Encode one statically typed aligned field; the root frame restores diagnostics after any failure."
@inline function encodealignedchild(plan::P, e::Encoder, value::T,
                                    schema::Schema, name::AbstractString) where {P<:WritePlan,T}
    previous = e.expected
    pushencodepath!(e, ENCODE_PATH_FIELD, name, 0)
    e.expected = schema
    encode(plan, e, value)  # `encode` attaches the live path before propagating EncodeError
    e.expected = previous
    popencodepath!(e)
    return nothing
end

@inline function encfields(e::Encoder, plans::Tuple, vals::Tuple, fields, i::Int)
    field = fields[i]
    encodealignedchild(first(plans), e, first(vals), field.schema, field.name)
    return encfields(e, Base.tail(plans), Base.tail(vals), fields, i + 1)
end

"""
One aligned row through the same `encode` methods, monomorphized: the record-level prelude of
`encode(::WritePlan, e, x)` plus `encodedatum!`'s datum-size check, then the fields in schema order.
"""
function encodealigned!(plans::Tuple, e::Encoder, x::NamedTuple,
                        schema::RecordSchema)
    return withencoderroot!(e, schema) do
        encodedwork!(e) do
            enter!(e)
            try
                countvalues!(e.budget)                    # the record value itself
                encfields(e, plans, values(x), schema.fields, 1)
            finally
                leave!(e)
            end
        end
    end
end

"Encode one aligned row without diagnostic path frames; callers replay failures diagnostically."
function encodealignedfast!(plans::Tuple, e::Encoder, x::NamedTuple)
    return encodedwork!(e) do
        enter!(e)
        try
            countvalues!(e.budget)
            encfieldsfast(e, plans, values(x))
        finally
            leave!(e)
        end
    end
end

function encodevalue(p::WDecimal, e::Encoder, x::DataDecimals.AbstractDecimal)
    return encodevalue(p, e, _decimalinput(x))
end
function encodevalue(p::WDuration, e::Encoder, x::Durations.Duration)
    0 <= x.months && 0 <= x.days && 0 <= x.nanoseconds ||
        encodeerror("Avro duration components must be nonnegative", x)
    q, r = divrem(x.nanoseconds, 1_000_000)
    iszero(r) && q <= typemax(UInt32) ||
        encodeerror("Avro duration requires UInt32 milliseconds without rounding", x)
    return encodevalue(p, e, Duration(x))
end
