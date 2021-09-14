mutable struct MapType <: SchemaType
    type::String
    values::Schema
    default::Any
end

==(x::MapType, y::MapType) = x.values == y.values
MapType(values::Schema, default=nothing) = MapType("map", values, default)
MapType() = MapType("map", "", nothing)
StructTypes.StructType(::Type{MapType}) = StructTypes.Mutable()

juliatype(x::MapType) = Dict{String, juliatype(x.values)}
schematype(::StructTypes.DictType, ::Type{A}) where {A <: AbstractDict{K, V}} where {K, V} = MapType(schematype(V))

function writevalue(B::Binary, MT::MapType, x, buf, pos, len, opts)
    xlen = length(x)
    pos = writevalue(B, long, -xlen, buf, pos, len, opts)
    xlen == 0 && return pos
    nb = nbytes(MT, x, true)
    pos = writevalue(B, long, nb, buf, pos, len, opts)
    pairs = StructTypes.keyvaluepairs(x)
    next = iterate(pairs)
    startpos = pos
    while next !== nothing
        (k, v), state = next
        pos = writevalue(B, string, k, buf, pos, len, opts)
        pos = writevalue(B, MT.values, v, buf, pos, len, opts)
        next = iterate(pairs, state)
    end
    @assert nb == (pos - startpos)
    # write out final 0 count block
    pos = writevalue(B, long, 0, buf, pos, len, opts)
    return pos
end

# number of bytes to write map in avro format, *not including* leading count/size longs
function nbytes(MT::MapType, x, self=false)
    nb = 0
    pairs = StructTypes.keyvaluepairs(x)
    next = iterate(pairs)
    while next !== nothing
        (k, v), state = next
        nb += nbytes(string, k)
        nb += nbytes(MT.values, v)
        next = iterate(pairs, state)
    end
    return nb == 0 ? 1 : self ? nb : nbytes(-length(x)) + nbytes(nb) + nb + 1
end

function readvalue(B::Binary, MT::MapType, ::Type{A}, buf, pos, buflen, opts) where {A <: AbstractDict{K, V}} where {K, V}
    map = A()
    while true
        sz = startpos = 0
        len, pos = readvalue(B, long, Int, buf, pos, buflen, opts)
        if len == 0
            break
        elseif len < 0
            sz, pos = readvalue(B, long, Int, buf, pos, buflen, opts)
            startpos = pos
            len = -len
        end
        for i = 1:len
            key, pos = readvalue(B, string, K, buf, pos, buflen, opts)
            val, pos = readvalue(B, MT.values, V, buf, pos, buflen, opts)
            map[key] = val
        end
        if sz > 0
            @assert sz == (pos - startpos)
        end
    end
    return map, pos
end

function skipvalue(B::Binary, MT::MapType, ::Type{A}, buf, pos, buflen, opts) where {A <: AbstractDict{K, V}} where {K, V}
    while true
        len, pos = readvalue(B, long, Int, buf, pos, buflen, opts)
        if len == 0
            break
        elseif len < 0
            sz, pos = readvalue(B, long, Int, buf, pos, buflen, opts)
            pos += sz
        else
            for i = 1:-len
                pos = skipvalue(B, string, K, buf, pos, buflen, opts)
                pos = skipvalue(B, MT.values, V, buf, pos, buflen, opts)
            end
        end
    end
    return pos
end
