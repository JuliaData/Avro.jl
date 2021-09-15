struct DecimalType <: LogicalType
    type::String
    logicalType::String
    size::Int64
    precision::Int64
    scale::Int64
end

DecimalType(type::String, logicalType::String, size::Union{Nothing, Int64}, precision::Int64, scale::Union{Nothing, Int64}) = DecimalType(type, logicalType, something(size, 16), precision, something(scale, 0))
DecimalType(precision::Integer, scale::Union{Nothing, Integer}) = DecimalType("fixed", "decimal", 16, precision, something(scale, 0))

const FIXEDDEC = FixedType("decimal", 16)

struct Decimal{S, P}
    value::Int128
end

juliatype(x::DecimalType) = Decimal{x.scale, x.precision}
schematype(::Type{Decimal{S, P}}) where {S, P} = DecimalType(P, S)

function writevalue(B::Binary, D::DecimalType, x::Decimal, buf, pos, len, opts)
    @assert D.type == "fixed"
    return writevalue(B, FIXEDDEC, _cast(NTuple{16, UInt8}, x.value), buf, pos, len, opts)
end

nbytes(::DecimalType, x::Decimal) = 16

inttype(sz) = sz == 1 ? Int8 : sz == 2 ? Int16 : sz == 4 ? Int32 : sz == 8 ? Int64 : Int128

function readvalue(B::Binary, D::DecimalType, ::Type{Decimal{S, P}}, buf, pos, len, opts) where {S, P}
    if D.type == "fixed"
        x, pos = readvalue(B, FixedType("", D.size), NTuple{D.size, UInt8}, buf, pos, len, opts)
        return Decimal{S, P}(Int128(_cast(inttype(D.size), x))), pos
    elseif D.type == "bytes"
        b, pos = readvalue(B, bytes, Vector{UInt8}, buf, pos, len, opts)
        resize!(b, 16)
        return Decimal{S, P}(reinterpret(Int128, b)[1]), pos
    else
        error("invalid Decimal type = $(D.type)")
    end
end

function skipvalue(B::Binary, D::DecimalType, ::Type{Decimal{S, P}}, buf, pos, len, opts) where {S, P}
    if D.type == "fixed"
        return skipvalue(B, FixedType("", D.size), NTuple{D.size, UInt8}, buf, pos, len, opts)
    elseif D.type == "bytes"
        return skipvalue(B, bytes, Vector{UInt8}, buf, pos, len, opts)
    else
        error("invalid Decimal type = $(D.type)")
    end
end

struct UUIDType <: LogicalType
    type::String
    logicalType::String
end
UUIDType() = UUIDType("string", "uuid")

juliatype(::UUIDType) = UUID
schematype(::Type{UUID}) = UUIDType()

writevalue(B::Binary, ::UUIDType, x::UUID, buf, pos, len, opts) =
    writevalue(B, string, Base.string(x), buf, pos, len, opts)

nbytes(::UUIDType, ::UUID) = nbytes(36) + 36

function readvalue(B::Binary, ::UUIDType, ::Type{UUID}, buf, pos, len, opts)
    x, pos = readvalue(B, string, String, buf, pos, len, opts)
    return UUID(x), pos
end

skipvalue(B::Binary, ::UUIDType, ::Type{UUID}, buf, pos, len, opts) =
    skipvalue(B, string, String, buf, pos, len, opts)

struct DateType <: LogicalType
    type::String
    logicalType::String
end
DateType() = DateType("int", "date")

juliatype(::DateType) = Date
schematype(::Type{Date}) = DateType()

const UNIX_EPOCH_DATE = Dates.value(Dates.Date(1970))
unix(x::Date) = Int32(Dates.value(x) - UNIX_EPOCH_DATE)
date(x::Int32) = Dates.Date(Dates.UTD(Int64(x + UNIX_EPOCH_DATE)))

writevalue(B::Binary, ::DateType, x::Date, buf, pos, len, opts) =
    writevalue(B, int, unix(x), buf, pos, len, opts)

nbytes(::DateType, x::Date) = nbytes(unix(x))

function readvalue(B::Binary, ::DateType, ::Type{Date}, buf, pos, len, opts)
    x, pos = readvalue(B, int, Int32, buf, pos, len, opts)
    return date(x), pos
end

skipvalue(B::Binary, ::DateType, ::Type{Date}, buf, pos, len, opts) =
    skipvalue(B, int, Int32, buf, pos, len, opts)

struct TimeMillisType <: LogicalType
    type::String
    logicalType::String
end
TimeMillisType() = TimeMillisType("int", "time-millis")

juliatype(::TimeMillisType) = Time

writevalue(B::Binary, ::TimeMillisType, x::Time, buf, pos, len, opts) =
    writevalue(B, long, div(Dates.value(x), 1000_000), buf, pos, len, opts)

nbytes(::TimeMillisType, x::Time) = nbytes(div(Dates.value(x), 1000_000))

function readvalue(B::Binary, ::TimeMillisType, ::Type{Time}, buf, pos, len, opts)
    x, pos = readvalue(B, int, Int32, buf, pos, len, opts)
    return Time(Nanosecond(Millisecond(x))), pos
end

skipvalue(B::Binary, ::TimeMillisType, ::Type{Time}, buf, pos, len, opts) =
    skipvalue(B, int, Int32, buf, pos, len, opts)

struct TimeMicrosType <: LogicalType
    type::String
    logicalType::String
end
TimeMicrosType() = TimeMicrosType("long", "time-micros")

juliatype(::TimeMicrosType) = Time
schematype(::Type{Time}) = TimeMicrosType()

writevalue(B::Binary, ::TimeMicrosType, x::Time, buf, pos, len, opts) =
    writevalue(B, long, div(Dates.value(x), 1000), buf, pos, len, opts)

nbytes(::TimeMicrosType, x::Time) = nbytes(div(Dates.value(x), 1000))

function readvalue(B::Binary, ::TimeMicrosType, ::Type{Time}, buf, pos, len, opts)
    x, pos = readvalue(B, long, Int64, buf, pos, len, opts)
    return Time(Nanosecond(Microsecond(x))), pos
end

skipvalue(B::Binary, ::TimeMicrosType, ::Type{Time}, buf, pos, len, opts) =
    skipvalue(B, long, Int64, buf, pos, len, opts)

struct TimestampMillisType <: LogicalType
    type::String
    logicalType::String
end
TimestampMillisType() = TimestampMillisType("long", "timestamp-millis")

juliatype(::TimestampMillisType) = DateTime

const UNIX_EPOCH_DATETIME = Dates.value(Dates.DateTime(1970))
unix(x::DateTime) = Int64(Dates.value(x) - UNIX_EPOCH_DATETIME)
datetime(x::Int64) = Dates.DateTime(Dates.UTM(Int64(x + UNIX_EPOCH_DATETIME)))

writevalue(B::Binary, ::TimestampMillisType, x::DateTime, buf, pos, len, opts) =
    writevalue(B, long, unix(x), buf, pos, len, opts)

nbytes(::TimestampMillisType, x::DateTime) = nbytes(unix(x))

function readvalue(B::Binary, ::TimestampMillisType, ::Type{DateTime}, buf, pos, len, opts)
    x, pos = readvalue(B, long, Int64, buf, pos, len, opts)
    return datetime(x), pos
end

skipvalue(B::Binary, ::TimestampMillisType, ::Type{DateTime}, buf, pos, len, opts) =
    skipvalue(B, long, Int64, buf, pos, len, opts)

struct TimestampMicrosType <: LogicalType
    type::String
    logicalType::String
end
TimestampMicrosType() = TimestampMicrosType("long", "timestamp-micros")

juliatype(::TimestampMicrosType) = DateTime

writevalue(B::Binary, ::TimestampMicrosType, x::DateTime, buf, pos, len, opts) =
    writevalue(B, long, unix(x) * 1000, buf, pos, len, opts)

nbytes(::TimestampMicrosType, x::DateTime) = nbytes(unix(x) * 1000)

function readvalue(B::Binary, ::TimestampMicrosType, ::Type{DateTime}, buf, pos, len, opts)
    x, pos = readvalue(B, long, Int64, buf, pos, len, opts)
    return datetime(div(x, 1000)), pos
end

skipvalue(B::Binary, ::TimestampMicrosType, ::Type{DateTime}, buf, pos, len, opts) =
    skipvalue(B, long, Int64, buf, pos, len, opts)

struct LocalTimestampMillisType <: LogicalType
    type::String
    logicalType::String
end
LocalTimestampMillisType() = LocalTimestampMillisType("long", "local-timestamp-millis")

juliatype(::LocalTimestampMillisType) = DateTime
schematype(::Type{DateTime}) = LocalTimestampMillisType()

writevalue(B::Binary, ::LocalTimestampMillisType, x::DateTime, buf, pos, len, opts) =
    writevalue(B, long, unix(x), buf, pos, len, opts)

nbytes(::LocalTimestampMillisType, x::DateTime) = nbytes(unix(x))

function readvalue(B::Binary, ::LocalTimestampMillisType, ::Type{DateTime}, buf, pos, len, opts)
    x, pos = readvalue(B, long, Int64, buf, pos, len, opts)
    return datetime(x), pos
end

skipvalue(B::Binary, ::LocalTimestampMillisType, ::Type{DateTime}, buf, pos, len, opts) =
    skipvalue(B, long, Int64, buf, pos, len, opts)

struct LocalTimestampMicrosType <: LogicalType
    type::String
    logicalType::String
end
LocalTimestampMicrosType() = LocalTimestampMicrosType("long", "local-timestamp-micros")

juliatype(::LocalTimestampMicrosType) = DateTime

writevalue(B::Binary, ::LocalTimestampMicrosType, x::DateTime, buf, pos, len, opts) =
    writevalue(B, long, unix(x) * 1000, buf, pos, len, opts)

nbytes(::LocalTimestampMicrosType, x::DateTime) = nbytes(unix(x) * 1000)

function readvalue(B::Binary, ::LocalTimestampMicrosType, ::Type{DateTime}, buf, pos, len, opts)
    x, pos = readvalue(B, long, Int64, buf, pos, len, opts)
    return datetime(div(x, 1000)), pos
end

skipvalue(B::Binary, ::LocalTimestampMicrosType, ::Type{DateTime}, buf, pos, len, opts) =
    skipvalue(B, long, Int64, buf, pos, len, opts)

struct DurationType <: LogicalType
    type::String
    logicalType::String
    size::Int64
end
DurationType() = DurationType("fixed", "duration", 12)

struct Duration
    months::Int32
    days::Int32
    millis::Int32
end

juliatype(::DurationType) = Duration
schematype(::Type{Duration}) = DurationType()
const FIXEDDUR = FixedType("duration", 12)

function writevalue(B::Binary, D::DurationType, x::Duration, buf, pos, len, opts)
    @assert D.type == "fixed"
    return writevalue(B, FIXEDDUR, _cast(NTuple{12, UInt8}, x), buf, pos, len, opts)
end

nbytes(::DurationType, x::Duration) = 12

function readvalue(B::Binary, ::DurationType, ::Type{Duration}, buf, pos, len, opts)
    x, pos = readvalue(B, FIXEDDUR, NTuple{12, UInt8}, buf, pos, len, opts)
    months, days, millis = _cast(NTuple{3, Int32}, x)
    return Duration(months, days, millis), pos
end

skipvalue(B::Binary, ::DurationType, ::Type{Duration}, buf, pos, len, opts) =
    skipvalue(B, FIXEDDUR, NTuple{12, UInt8}, buf, pos, len, opts)
