"""
    Avro.write([filename|io,] x::T; kw...)

Write an object `x` of avro-supported type `T` in the avro format.
If a file name is provided as a `String` as 1st argument, the avro
data will be written out to disk. Similarly, an `IO` argument can be
provided as 1st argument. If no destination 1st argument is provided,
a byte buffer `Vector{UInt8}` will be returned with avro data written
to it. Supported keyword arguments include:
  * `schema`: the type that should be used when encoding the object in
  the avro format; most common is providing a `Union{...}` type to write
  the data out specifically as a "union type" instead of only the type of the object;
  alternatively, a valid `Avro.Schema` can be passed, like the result of
  `Avro.parseschema(src)`
"""
function write end

function write(obj::T;
        schema=schematype(T),
        jsonencoding::Bool=false,
        kw...) where {T}
    E = jsonencoding ? JSON() : Binary()
    pos = 1
    sch = schematype(schema)
    buf = Vector{UInt8}(undef, nbytes(sch, obj))
    pos = writevalue(E, sch, obj, buf, pos, length(buf), kw)
    return buf
end

function write(io::IO, obj::T; kw...) where {T}
    buf = write(obj; kw...)
    return Base.write(io, buf)
end

function write(fname::String, obj::T; kw...) where {T}
    open(fname, "w") do io
        write(io, obj; kw...)
    end
    fname
end

"""
    Avro.read(source, T_or_sch) => T

Read an avro-encoded object of type `T` or avro schema `sch` from `source`,
which can be a byte buffer `AbstractVector{UInt8}`, file name `String`, or `IO`.

The data in `source` must be avro-formatted data, as no schema verification
can be done. Note that "avro object container files" should be processed
using [`Avro.readtable`](@ref) instead, where the data schema is encoded
in the file itself. Also note that the 2nd argument can be a Julia type like
`Vector{String}`, or a valid `Avro.Schema` type object, like is returned from
`Avro.parseschema(src)`.
"""
function read end

function read(buf::AbstractVector{UInt8}, ::Type{T}; schema=schematype(T), jsonencoding::Bool=false, kw...) where {T}
    x, pos = readvalue(jsonencoding ? JSON() : Binary(), schematype(schema), T, buf, 1, length(buf), kw)
    return x
end

function read(buf::AbstractVector{UInt8}, sch::Schema; T=juliatype(sch), jsonencoding::Bool=false, kw...)
    x, pos = readvalue(jsonencoding ? JSON() : Binary(), sch, T, buf, 1, length(buf), kw)
    return x
end

read(fname::String, sch; kw...) = read(Mmap.mmap(fname), sch; kw...)
read(io::IO, sch; kw...) = read(Base.read(io), sch; kw...)
read(io::IOBuffer, sch; kw...) = read(io.data, sch; kw...)

# fallbacks
writevalue(E::Encoding, x::T, buf, pos, len, opts) where {T} =
    writevalue(E, schematype(T), x, buf, pos, len, opts)
readvalue(E::Encoding, ::Type{T}, buf, pos, len, opts) where {T} =
    readvalue(E, schematype(T), T, buf, pos, len, opts)
readvalue(B::Binary, sch::String, ::Type{T}, buf, pos, len, opts) where {T} =
    readvalue(B, PrimitiveType(sch), T, buf, pos, len, opts)
skipvalue(E::Encoding, ::Type{T}, buf, pos, len, opts) where {T} =
    skipvalue(E, schematype(T), T, buf, pos, len, opts)
skipvalue(B::Binary, sch::String, ::Type{T}, buf, pos, len, opts) where {T} =
    skipvalue(B, PrimitiveType(sch), T, buf, pos, len, opts)

nbytes(x::T) where {T} = nbytes(schematype(T), x)

# null
writevalue(::Binary, ::NullType, x, buf, pos, len, opts) = pos
readvalue(::Binary, ::NullType, ::Type{T}, buf, pos, len, opts) where {T} = (StructTypes.construct(T), pos)
skipvalue(::Binary, ::NullType, ::Type{T}, buf, pos, len, opts) where {T} = pos
nbytes(::NullType, T) = 0

# boolean
function writevalue(::Binary, ::BooleanType, x, buf, pos, len, opts)
    @check 1
    @inbounds buf[pos] = UInt8(Bool(x))
    return pos + 1
end

function readvalue(::Binary, ::BooleanType, ::Type{T}, buf, pos, len, opts) where {T}
    @readcheck 1
    @inbounds x = buf[pos]
    return StructTypes.construct(T, Bool(x)), pos + 1
end

skipvalue(::Binary, ::BooleanType, T, buf, pos, len, opts) = pos + 1
nbytes(::BooleanType, T) = 1

# generic fallbacks to convert to supported number type
const NumberType = Union{IntType, LongType}
const FloatTypes = Union{FloatType, DoubleType}

writevalue(B::Binary, S::NumberType, y::T, buf, pos, len, opts) where {T} =
    writevalue(B, S, StructTypes.construct(StructTypes.numbertype(T), y), buf, pos, len, opts)

function readvalue(B::Binary, S::Union{NumberType, FloatTypes}, ::Type{T}, buf, pos, len, opts) where {T}
    x, pos = _readvalue(B, S, StructTypes.numbertype(T), buf, pos, len, opts)
    return StructTypes.construct(T, x), pos
end

# read/write integers in vint zigzag format: https://lucene.apache.org/core/3_5_0/fileformats.html#VInt
writevalue(B::Binary, N::NumberType, y::T, buf, pos, len, opts) where {T <: Unsigned} =
    writevalue(B, N, signed(widen(y)), buf, pos, len, opts)

function writevalue(::Binary, ::NumberType, y::T, buf, pos, len, opts) where {T <: Signed}
    x = tozigzag(y)
    while true
        # are any bits higher than least 7 set? if not, we're done
        if (x & ((~T(0)) << 7)) == 0
            @inbounds buf[pos] = x % UInt8
            return pos + 1
        end
        @inbounds buf[pos] = ((x & 0b1111111) | 0b10000000) % UInt8
        pos += 1
        x >>>= 7
    end
end

function nbytes(::NumberType, y::T) where {T <: Integer}
    x = tozigzag(y)
    N = 1
    while true
        # are any bits higher than least 7 set? if not, we're done
        if (x & ((~T(0)) << 7)) == 0
            return N
        end
        x >>>= 7
        N += 1
    end
end

function _readvalue(B::Binary, N::NumberType, ::Type{T}, buf, pos, len, opts) where {T <: Unsigned}
    x, pos = _readvalue(B, N, signed(T), buf, pos, len, opts)
    return Core.bitcast(T, x), pos
end

function _readvalue(::Binary, ::NumberType, ::Type{T}, buf, pos, len, opts) where {T <: Signed}
    x = T(0)
    shift = 0
    len = length(buf)
    while pos <= len
        @inbounds b = buf[pos]
        pos += 1
        x |= T(b & 0x7F) << shift
        shift += 7
        (b & 0x80) == 0 && return fromzigzag(x), pos
    end
    return x, pos
end

function skipvalue(::Binary, ::NumberType, ::Type{T}, buf, pos, len, opts) where {T <: Integer}
    len = length(buf)
    @inbounds while pos <= len && (buf[pos] & 0x80) > 0
        pos += 1
    end
    return pos + 1
end

# float/double

function writevalue(::Binary, ::FloatTypes, x::T, buf, pos, len, opts) where {T <: Base.IEEEFloat}
    N = sizeof(T)
    @check N
    ref = Ref(x)
    GC.@preserve ref unsafe_copyto!(pointer(buf, pos), convert(Ptr{UInt8}, Base.unsafe_convert(Ref{T}, ref)), N)
    return pos + N
end

nbytes(::FloatTypes, x::T) where {T <: Base.IEEEFloat} = sizeof(T)

function _readvalue(::Binary, ::FloatTypes, ::Type{T}, buf, pos, len, opts) where {T <: Base.IEEEFloat}
    @readcheck sizeof(T)
    GC.@preserve buf begin
        ptr::Ptr{T} = pointer(buf, pos)
        x = unsafe_load(ptr)
    end
    return x, pos + sizeof(T)
end

skipvalue(::Binary, ::FloatTypes, ::Type{T}, buf, pos, len, opts) where {T <: Base.IEEEFloat} = pos + sizeof(T)

# bytes/strings
_codeunits(x::AbstractVector{UInt8}) = x
_codeunits(x) = codeunits(Base.string(x))

# generic fallbacks to convert to string type
writevalue(B::Binary, S::StringType, x, buf, pos, len, opts) =
    _writevalue(B, S, Base.string(x), buf, pos, len, opts)
writevalue(B::Binary, S::BytesType, x, buf, pos, len, opts) =
    _writevalue(B, S, x, buf, pos, len, opts)

const BytesOrString = Union{BytesType, StringType}

function _writevalue(B::Binary, ::BytesOrString, x, buf, pos, len, opts)
    N = sizeof(x)
    pos = writevalue(B, long, N, buf, pos, len, opts)
    @check N
    for b in _codeunits(x)
        @inbounds buf[pos] = b
        pos += 1
    end
    return pos
end

function _writevalue(B::Binary, ::BytesType, x::AbstractVector{UInt8}, buf, pos, len, opts)
    N = sizeof(x)
    pos = writevalue(B, long, N, buf, pos, len, opts)
    @check N
    copyto!(buf, pos, x, 1, N)
    return pos + N
end

nbytes(::BytesOrString, x) = nbytes(long, sizeof(_codeunits(x))) + sizeof(_codeunits(x))

function readvalue(B::Binary, ::BytesType, ::Type{T}, buf, pos, len, opts) where {T}
    N, pos = readvalue(B, long, Int64, buf, pos, len, opts)
    @readcheck N
    return StructTypes.construct(T, view(buf, pos:(pos + N - 1))), N + pos
end

function skipvalue(B::Binary, ::BytesOrString, ::Type{T}, buf, pos, len, opts) where {T}
    N, pos = readvalue(B, long, Int64, buf, pos, len, opts)
    return N + pos
end

function readvalue(B::Binary, ::StringType, ::Type{T}, buf, pos, len, opts) where {T}
    N, pos = readvalue(B, long, Int64, buf, pos, len, opts)
    @readcheck N
    return StructTypes.construct(T, pointer(buf, pos), N), N + pos
end
