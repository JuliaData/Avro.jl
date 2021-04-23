mutable struct FixedType <: NamedType
    type::String
    namespace::String
    name::String
    aliases::Union{Vector{String}, Nothing}
    size::Int
end

==(x::FixedType, y::FixedType) = x.name == y.name && x.size == y.size
FixedType(name, size, namespace="", aliases=nothing) =
    FixedType("fixed", namespace, name, aliases, size)
FixedType() = FixedType("fixed", "", "", nothing, 0)
StructTypes.StructType(::Type{FixedType}) = StructTypes.Mutable()

juliatype(x::FixedType) = NTuple{x.size, UInt8}
schematype(::StructTypes.ArrayType, ::Type{NTuple{N, UInt8}}, name="Fixed_$N") where {N} =
    FixedType(name, N)

function writevalue(::Binary, ::FixedType, x::NTuple{N, UInt8}, buf, pos, len, opts) where {N}
    @check N
    for i = 1:N
        @inbounds buf[pos] = x[i]
        pos += 1
    end
    return pos
end

nbytes(::FixedType, x::NTuple{N, UInt8}) where {N} = N

mutable struct FixedClosure
    pos::Int
end

@inline function (f::FixedClosure)(i::Int, buf)
    @inbounds b = buf[f.pos]
    f.pos += 1
    return b
end

function readvalue(::Binary, ::FixedType, ::Type{NTuple{N, UInt8}}, buf, pos, len, opts) where {N}
    f = FixedClosure(pos)
    x = ntuple(i -> f(i, buf), Val(N))
    return x, f.pos
end

skipvalue(::Binary, ::FixedType, ::Type{NTuple{N, UInt8}}, buf, pos, len, opts) where {N} = pos + N
