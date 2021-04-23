mutable struct EnumType <: NamedType
    type::String
    namespace::String
    name::String
    aliases::Union{Vector{String}, Nothing}
    doc::Union{String, Nothing}
    symbols::Vector{String}
    default::Union{String, Nothing}
end

==(x::EnumType, y::EnumType) = x.name == y.name && x.symbols == y.symbols
EnumType(name, symbols, namespace="", aliases=nothing, doc=nothing, default=nothing) =
    EnumType("enum", namespace, name, aliases, doc, symbols, default)
EnumType() = EnumType("enum", "", "", nothing, nothing, String[], nothing)
StructTypes.StructType(::Type{EnumType}) = StructTypes.Mutable()

struct EnumStructType <: StructTypes.StructType end

struct Enum{names}
    value::Int
end

function Base.show(io::IO, x::Enum{names}) where {names}
    print(io, names[x.value + 1], " = ", x.value)
end

StructTypes.StructType(::Type{<:Enum}) = EnumStructType()

juliatype(x::EnumType) = Enum{symtup(x.symbols)}

function sym(x)
    Base.isidentifier(x) || error("invalid avro enum value: `$x`")
    return String(x)
end

schematype(::EnumStructType, ::Type{Enum{names}}, name=Base.string("Enum_", join(names, "_"))) where {names} =
    EnumType(name, [sym(nm) for nm in names])

function writevalue(B::Binary, ::EnumType, x, buf, pos, len, opts)
    return writevalue(B, long, x.value, buf, pos, len, opts)
end

nbytes(::EnumType, x::Enum) = nbytes(x.value)

function readvalue(B::Binary, ::EnumType, ::Type{Enum{names}}, buf, pos, len, opts) where {names}
    x, pos = readvalue(B, long, Int, buf, pos, len, opts)
    return Enum{names}(x), pos
end

skipvalue(B::Binary, ::EnumType, ::Type{Enum{names}}, buf, pos, len, opts) where {names} =
    skipvalue(B, long, Int, buf, pos, len, opts)
