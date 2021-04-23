function juliatype(x::AbstractVector)
    Union{map(juliatype, x)...}
end

# iterate a Julia Union{...} type, producing an array of unioned types
Base.@pure function eachunion(T, elems)
    if elems === nothing
        elems = Type[]
    end
    if T isa Union
        if T.a isa Union
            push!(elems, T.b)
            return eachunion(T.a, elems)
        else
            push!(elems, T.a)
            return eachunion(T.b, elems)
        end
    else
        push!(elems, T)
        return Tuple{elems...}
    end
end

function schematype(::StructTypes.Struct, U::Union)
    T = eachunion(U, nothing)
    sch = Vector{Union{String, SchemaType, LogicalType}}(undef, fieldcount(T))
    for i = 1:fieldcount(T)
        sch[i] = schematype(fieldtype(T, i))
    end
    return sch
end

function unionindex(T, UT)
    for i = 1:length(UT)
        T == UT[i] && return (i - 1)
    end
    error("value with avro schema $T not valid for union type $UT")
end

function writevalue(B::Binary, UT::UnionType, x::T, buf, pos, len, opts) where {T}
    sch = schematype(T)
    i = unionindex(sch, UT)
    pos = writevalue(B, long, i, buf, pos, len, opts)
    return writevalue(B, sch, x, buf, pos, len, opts)
end

function nbytes(UT::UnionType, x::T) where {T}
    sch = schematype(T)
    i = unionindex(sch, UT)
    n = nbytes(long, i)
    return n + nbytes(sch, x)
end

function readvalue(B::Binary, UT::UnionType, ::Type{T}, buf, pos, len, opts) where {T}
    i, pos = readvalue(B, long, Int, buf, pos, len, opts)
    sch = UT[i + 1]
    S = fieldtype(eachunion(T, nothing), i + 1)
    x, pos = readvalue(B, sch, S, buf, pos, len, opts)
    return x, pos
end

function skipvalue(B::Binary, UT::UnionType, ::Type{T}, buf, pos, len, opts) where {T}
    i, pos = readvalue(B, long, Int, buf, pos, len, opts)
    sch = UT[i + 1]
    S = fieldtype(eachunion(T, nothing), i + 1)
    pos = skipvalue(B, sch, S, buf, pos, len, opts)
    return pos
end
