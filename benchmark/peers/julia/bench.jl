# usage: julia --project=julia -t N bench.jl read  <table|table-typed|rows-typed|rows-generic> <shape> <file> <iters>
#        julia --project=julia -t 1 bench.jl write <table|writer> <shape> <infile> <outfile> <codec> <iters>
# Prints `load <s>`, then per iteration `iter <k> <s> <rows> <isum> <nstr> <nbytes> key=value...`.
const T0 = time_ns()
using Avro, Tables, Dates
println("load ", (time_ns() - T0) / 1e9)

const M61 = (Int64(1) << 61) - 1

@enum Kind CLICK VIEW PURCHASE SIGNUP LOGOUT

struct NumericRow
    a::Int64; b::Int64; c::Int64; d::Int64; x::Float64; y::Float64; n::Int32; flag::Bool
end
struct StringsRow
    s0::String; s1::String; s2::String; s3::String; s4::String; s5::String; s6::String; s7::String; s8::String; s9::String
end
struct EventRow
    id::Int64; ts::Avro.Timestamp{Microsecond}; name::String; user::Union{Missing,String}; value::Float64
    count::Int32; kind::Kind; parent::Union{Missing,Int64}; ok::Bool; tag::Vector{UInt8}
end
struct Point
    label::String; w::Float64
end
struct NestedRow
    id::Int64; items::Vector{Int64}; attrs::Dict{String,Int64}; point::Point
end
rowtype(shape) = shape == "numeric" ? NumericRow : shape == "strings" ? StringsRow :
                 shape == "mixed" ? EventRow : shape == "nested" ? NestedRow : error("unknown shape $shape")

# The checksum every implementation computes: integers, enum ordinals, booleans and the IEEE-754 bit
# patterns of doubles summed modulo 2^61; strings, map keys and bytes counted with their byte lengths.
mutable struct Ck
    rows::Int64; isum::Int64; nstr::Int64; nbytes::Int64
end
Ck() = Ck(0, 0, 0, 0)

ck!(c::Ck, x::Union{Int64,Int32,Bool}) = (c.isum += x; nothing)
ck!(c::Ck, x::Float64) = (c.isum += reinterpret(Int64, x); nothing)
ck!(c::Ck, x::Union{String,Vector{UInt8}}) = (c.nstr += 1; c.nbytes += sizeof(x); nothing)
ck!(c::Ck, x::AbstractString) = (c.nstr += 1; c.nbytes += ncodeunits(x); nothing)   # a DataString column
ck!(c::Ck, ::Missing) = nothing
ck!(c::Ck, x::Avro.EnumValue) = (c.isum += Avro.ordinal(x); nothing)
ck!(c::Ck, x::Kind) = (c.isum += Integer(x); nothing)
ck!(c::Ck, x::Avro.Timestamp) = (c.isum += Dates.value(x); nothing)
ck!(c::Ck, x::Vector) = (for v in x; ck!(c, v); end; nothing)
ck!(c::Ck, x::Dict) = (for (k, v) in x; ck!(c, k); ck!(c, v); end; nothing)
ck!(c::Ck, x::Union{Point,NumericRow,StringsRow,EventRow,NestedRow}) = ckfields!(c, x)
ck!(c::Ck, r::Avro.Record) = (for i in 1:length(Tables.columnnames(r)); ckany!(c, Tables.getcolumn(r, i)); end; nothing)

@generated function ckfields!(c::Ck, x::T) where {T}
    return Expr(:block, (:(ck!(c, getfield(x, $i))) for i in 1:fieldcount(T))..., nothing)
end

# Generic values arrive as `Any`; split on the concrete types this benchmark's schemas decode to.
function ckany!(c::Ck, @nospecialize(v))
    if v isa Int64
        ck!(c, v)
    elseif v isa String
        ck!(c, v)
    elseif v isa Float64
        ck!(c, v)
    elseif v isa Int32
        ck!(c, v)
    elseif v isa Bool
        ck!(c, v)
    elseif v isa Missing
    elseif v isa Avro.EnumValue
        ck!(c, v)
    elseif v isa Avro.Timestamp{Microsecond}
        ck!(c, v)
    elseif v isa Vector{UInt8}
        ck!(c, v)
    elseif v isa Vector{Int64}
        ck!(c, v)
    elseif v isa Dict{String,Int64}
        ck!(c, v)
    elseif v isa Avro.Record
        ck!(c, v)
    elseif v isa AbstractString
        ck!(c, v)
    else
        error("unexpected value of type $(typeof(v))")
    end
    return nothing
end

ckcol!(c::Ck, col::AbstractVector) = (for v in col; ck!(c, v); end; nothing)
ckcol!(c::Ck, col::AbstractVector{Any}) = (for v in col; ckany!(c, v); end; nothing)

function read_table(path, ntasks)
    t = Avro.Table(path; ntasks=ntasks)
    c = Ck()
    c.rows = length(t)
    for i in 1:length(Tables.columnnames(t))
        ckcol!(c, t[i])
    end
    return c
end

function read_table_typed(path, ::Type{T}, ntasks) where {T}
    t = Avro.Table(path, T; ntasks=ntasks)
    c = Ck()
    c.rows = length(t)
    for i in 1:length(Tables.columnnames(t))
        ckcol!(c, t[i])
    end
    return c
end

function read_typed(path, ::Type{T}) where {T}
    c = Ck()
    rows = Avro.Rows(path, T)
    try
        for r in rows
            c.rows += 1
            ckfields!(c, r)
        end
    finally
        close(rows)
    end
    return c
end

function read_generic(path)
    c = Ck()
    rows = Avro.Rows(path)
    try
        for r in rows
            c.rows += 1
            ck!(c, r)
        end
    finally
        close(rows)
    end
    return c
end

function report(k, st, rows, isum, nstr, nbytes, extra="")
    println("iter ", k, " ", st.time, " ", rows, " ", isum & M61, " ", nstr, " ", nbytes,
            " gc=", st.gctime, " alloc=", st.bytes, extra)
    flush(stdout)
end

function readmain(mode, shape, path, iters)
    T = rowtype(shape)
    for k in 1:iters
        st = mode == "table" ? (@timed read_table(path, Threads.nthreads())) :
             mode == "table-typed" ? (@timed read_table_typed(path, T, Threads.nthreads())) :
             mode == "rows-typed" ? (@timed read_typed(path, T)) :
             mode == "rows-generic" ? (@timed read_generic(path)) : error("unknown read mode $mode")
        c = st.value
        report(k, st, c.rows, c.isum, c.nstr, c.nbytes)
    end
end

function loadrows(path, ::Type{T}) where {T}
    rows = Avro.Rows(path, T)
    v = collect(rows)
    close(rows)
    return v
end

function columnsof(v::Vector{T}) where {T}
    cols = ntuple(Val(fieldcount(T))) do i
        col = Vector{fieldtype(T, i)}(undef, length(v))
        for j in eachindex(v)
            col[j] = getfield(v[j], i)
        end
        col
    end
    return NamedTuple{fieldnames(T)}(cols)
end

write_table(out, cols, schema, codec) = Avro.write(out, cols; schema=schema, codec=codec)

function write_writer(out, v, schema, codec)
    Avro.Writer(out, schema; codec=codec) do w
        for r in v
            push!(w, r)
        end
    end
end

function writemain(mode, shape, infile, outfile, codec, iters)
    schema = Avro.parseschema(read(joinpath(@__DIR__, "..", "schemas", shape * ".avsc"), String))
    v = loadrows(infile, rowtype(shape))
    cols = columnsof(v)
    GC.gc()
    for k in 1:iters
        st = mode == "table" ? (@timed write_table(outfile, cols, schema, Symbol(codec))) :
             mode == "writer" ? (@timed write_writer(outfile, v, schema, Symbol(codec))) :
             error("unknown write mode $mode")
        report(k, st, length(v), 0, 0, 0, " out=$(filesize(outfile))")
    end
end

function main(args)
    if args[1] == "read"
        readmain(args[2], args[3], args[4], parse(Int, args[5]))
    elseif args[1] == "write"
        writemain(args[2], args[3], args[4], args[5], args[6], parse(Int, args[7]))
    else
        error("unknown op $(args[1])")
    end
end

abspath(PROGRAM_FILE) == @__FILE__() && main(ARGS)
