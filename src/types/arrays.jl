mutable struct ArrayType <: SchemaType
    type::String
    items::Schema
    default::Any
end

==(x::ArrayType, y::ArrayType) = x.items == y.items
ArrayType(items::Schema, default=nothing) = ArrayType("array", items, default)
ArrayType() = ArrayType("array", "", nothing)
StructTypes.StructType(::Type{ArrayType}) = StructTypes.Mutable()

struct Array{T} <: AbstractVector{T}
    data::Vector{UInt8}
    values::Vector{Int}
end

Base.IndexStyle(::Type{<:Array}) = Base.IndexLinear()
Base.size(x::Array) = (length(x.values),)
Base.getindex(x::Array{T}, i::Int) where {T} =
    readvalue(Binary(), T, x.data, x.values[i], length(x.data), (;))[1]

function Base.copy(x::Array{T}) where {T}
    len = length(x)
    y = Vector{T}(undef, len)
    for i = 1:len
        @inbounds y[i] = copy(x[i])
    end
    return y
end

juliatype(x::ArrayType) = Array{juliatype(x.items)}
schematype(::StructTypes.ArrayType, ::Type{A}) where {A} = ArrayType(schematype(eltype(A)))

function writevalue(B::Binary, A::ArrayType, x, buf, pos, len, opts)
    xlen = length(x)
    pos = writevalue(B, long, -xlen, buf, pos, len, opts)
    xlen == 0 && return pos
    nb = nbytes(A, x, true)
    pos = writevalue(B, long, nb, buf, pos, len, opts)
    startpos = pos
    for val in x
        pos = writevalue(B, A.items, val, buf, pos, len, opts)
    end
    @assert nb == (pos - startpos)
    # write out final 0 count block
    pos = writevalue(B, long, 0, buf, pos, len, opts)
    return pos
end

function nbytes(A::ArrayType, x, self=false)
    if isempty(x)
        return self ? 0 : 1
    else
        nb = sum(y -> nbytes(A.items, y), x)
        return self ? nb : nbytes(length(x)) + nbytes(nb) + nb + 1
    end
end

function readvalue(B::Binary, AT::ArrayType, ::Type{A}, buf, pos, buflen, opts) where {A <: AbstractVector{T}} where {T}
    sz = startpos = 0
    len, pos = readvalue(B, long, Int, buf, pos, buflen, opts)
    if len == 0
        return StructTypes.construct(A, Array{T}(buf, Int[])), pos
    elseif len < 0
        sz, pos = readvalue(B, long, Int, buf, pos, buflen, opts)
        startpos = pos
        len = -len
    end
    values = Vector{Int}(undef, len)
    for i = 1:len
        @inbounds values[i] = pos
        pos = skipvalue(B, AT.items, T, buf, pos, buflen, opts)
    end
    if sz > 0
        @assert sz == (pos - startpos)
    end
    sz = startpos = 0
    len, pos = readvalue(B, long, Int, buf, pos, buflen, opts)
    len == 0 && return StructTypes.construct(A, Array{T}(buf, values)), pos
    # multiple blocks
    arr = ChainedVector([StructTypes.construct(A, Array{T}(buf, values))])
    while true
        if len < 0
            sz, pos = readvalue(B, long, Int, buf, pos, buflen, opts)
            startpos = pos
            len = -len
        end
        values = Vector{Int}(undef, len)
        for i = 1:len
            @inbounds values[i] = pos
            pos = skipvalue(B, AT.items, T, buf, pos, buflen, opts)
        end
        if sz > 0
            @assert sz == (pos - startpos)
        end
        append!(arr, StructTypes.construct(A, Array{T}(buf, values)))
        len, pos = readvalue(B, long, Int, buf, pos, buflen, opts)
        len == 0 && return arr, pos
    end
end

function skipvalue(B::Binary, AT::ArrayType, ::Type{A}, buf, pos, buflen, opts) where {A <: AbstractVector{T}} where {T}
    while true
        len, pos = readvalue(B, long, Int, buf, pos, buflen, opts)
        if len == 0
            break
        elseif len < 0
            sz, pos = readvalue(B, long, Int, buf, pos, buflen, opts)
            pos += sz
        else
            for i = 1:-len
                pos = skipvalue(B, AT.items, T, buf, pos, buflen, opts)
            end
        end
    end
    return pos
end
