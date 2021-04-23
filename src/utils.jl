@noinline buffertoosmall(len) = throw(ArgumentError("internal writing error: buffer too small, len = $len"))
@noinline unexpectedeof(len) = throw(ArgumentError("unexpected eof reading avro data: $len"))

# can turn off since we'll almost always control the buffer size when writing?
const WRITE_BUF_CHECK = true

macro check(n)
    esc(quote
        if $WRITE_BUF_CHECK
            (pos + $n - 1) > len && buffertoosmall(len)
        end
    end)
end

macro readcheck(n)
    esc(quote
        (pos + $n - 1) > len && unexpectedeof(len)
    end)
end

function tozigzag(x::T) where {T <: Integer}
    return xor(x << 1, x >> (8 * sizeof(T) - 1))
end

function fromzigzag(x::T) where {T <: Integer}
    return xor(x >> 1, -(x & T(1)))
end

symtup(x) = Tuple(Symbol(nm) for nm in x)

function _cast(::Type{Y}, x)::Y where {Y}
    y = Ref{Y}()
    _unsafe_cast!(y, Ref(x), 1)
    return y[]
end

function _unsafe_cast!(y::Ref{Y}, x::Ref, n::Integer) where {Y}
    X = eltype(x)
    GC.@preserve x y begin
        ptr_x = Base.unsafe_convert(Ptr{X}, x)
        ptr_y = Base.unsafe_convert(Ptr{Y}, y)
        unsafe_copyto!(Ptr{X}(ptr_y), ptr_x, n)
    end
    return y
end

"""
    Avro.tobuffer(tbl; kw...)

Take a Tables.jl-compatible input `tbl` and call `Avro.writetable`
with an `IOBuffer`, which is returned, with position at the beginning.
"""
function tobuffer(data; kwargs...)
    io = IOBuffer()
    writetable(io, data; kwargs...)
    seekstart(io)
    return io
end

"""
    Avro.parseschema(file_or_jsonstring)

Parse the avro schema in a file or raw json string. The schema is
expected to follow the format as described in the official [spec](http://avro.apache.org/docs/current/spec.html#schemas).
Returns a "schema type" that can be passed to `Avro.read(buf, sch)` as
the 2nd argument.
"""
function parseschema(file)
    buf = isfile(file) ? Base.read(file) : codeunits(file)
    return JSON3.read(buf, Avro.Schema)
end