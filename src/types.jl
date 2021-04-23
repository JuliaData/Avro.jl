abstract type Encoding end
struct Binary <: Encoding end
struct JSON <: Encoding end

# types for parsing schemas
abstract type SchemaType end

StructTypes.StructType(::Type{SchemaType}) = StructTypes.AbstractType()
StructTypes.omitempties(::Type{<:SchemaType}) = true
StructTypes.StructType(::Type{<:SchemaType}) = StructTypes.Struct()

StructTypes.subtypes(::Type{SchemaType}) = (
    null=NullType,
    boolean=BooleanType,
    int=IntType,
    long=LongType,
    float=FloatType,
    double=DoubleType,
    bytes=BytesType,
    string=StringType,
    record=RecordType,
    enum=EnumType,
    array=ArrayType,
    map=MapType,
    fixed=FixedType
)

abstract type LogicalType end
StructTypes.StructType(::Type{LogicalType}) = StructTypes.AbstractType()
StructTypes.subtypekey(::Type{LogicalType}) = :logicalType
StructTypes.StructType(::Type{<:LogicalType}) = StructTypes.Struct()

StructTypes.subtypes(::Type{LogicalType}) = NamedTuple{(
    :decimal,
    :uuid,
    :date,
    Symbol("time-millis"),
    Symbol("time-micros"),
    Symbol("timestamp-millis"),
    Symbol("timestamp-micros"),
    Symbol("local-timestamp-millis"),
    Symbol("local-timestamp-micros"),
    :duration
)}((
    DecimalType,
    UUIDType,
    DateType,
    TimeMillisType,
    TimeMicrosType,
    TimestampMillisType,
    TimestampMicrosType,
    LocalTimestampMillisType,
    LocalTimestampMicrosType,
    DurationType
))

const UnionType = Vector{Union{String, SchemaType, LogicalType}}
const Schema = Union{String, LogicalType, SchemaType, UnionType}

StructTypes.StructType(::Type{Schema}) = StructTypes.Struct()

abstract type PrimitiveType <: SchemaType end

for (sym, JT) in (("Null", Missing), ("Boolean", Bool),
                  ("Int", Int32), ("Long", Int64),
                  ("Float", Float32), ("Double", Float64),
                  ("Bytes", Vector{UInt8}), ("String", String))
    T = Symbol(sym, :Type)
    lower = lowercase(sym)
    @eval begin
        struct $T <: PrimitiveType
            type::String
        end
        const $(Symbol(lower)) = $T($lower)
        juliatype(::$T) = $JT
    end
end

==(x::PrimitiveType, y::String) = x.type == y
==(x::String, y::PrimitiveType) = x == y.type

juliatype(s::String) = s == "null" ? Missing :
                       s == "boolean" ? Bool :
                       s == "int" ? Int32 :
                       s == "long" ? Int64 :
                       s == "float" ? Float32 :
                       s == "double" ? Float64 :
                       s == "bytes" ? Vector{UInt8} : String

PrimitiveType(x::String) = schematype(juliatype(x))

function schematype end

schematype(sch::Union{SchemaType, UnionType, LogicalType}) = sch
schematype(::Type{T}) where {T} = schematype(StructTypes.StructType(T), T)

schematype(::StructTypes.NullType, T) = null
schematype(::StructTypes.BoolType, T) = boolean
schematype(S::StructTypes.NumberType, T) = schematype(S, StructTypes.numbertype(T))
schematype(::StructTypes.NumberType, U::Union) = schematype(StructTypes.Struct(), U)
schematype(::StructTypes.NumberType, ::Type{T}) where {T <: Integer} = int
schematype(::StructTypes.NumberType, ::Type{T}) where {T <: Union{Int64, UInt64}} = long
schematype(::StructTypes.NumberType, ::Type{Float32}) = float
schematype(::StructTypes.NumberType, ::Type{Float64}) = double
schematype(::StructTypes.ArrayType, ::Type{<:AbstractVector{UInt8}}) = bytes
schematype(::StructTypes.StringType, T) = string

abstract type NamedType <: SchemaType end

function fullname(x::NamedType, enclosing_namespace="")
    # If the name specified contains a dot, then it is assumed to be a fullname,
    # and any namespace also specified is ignored
    any(==(UInt8('.')), codeunits(x.name)) && return x.name
    # if namespace is empty, use enclosing (if any)
    isempty(x.namespace) && return Base.string(enclosing_namespace, x.name)
    return Base.string(x.namespace, x.name)
end

include("types/binary.jl")
include("types/records.jl")
include("types/enums.jl")
include("types/arrays.jl")
include("types/maps.jl")
include("types/fixed.jl")
include("types/unions.jl")
include("types/logical.jl")
include("types/rows.jl")
