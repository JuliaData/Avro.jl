const MAGIC = (UInt8('O'), UInt8('b'), UInt8('j'), 0x01)
const FileHeader = Record{(:magic, :meta, :sync), Tuple{NTuple{4, UInt8}, Dict{String, Vector{UInt8}}, NTuple{16, UInt8}}, 3}
const FileHeaderRecordType = schematype(FileHeader)

struct Block
    count::Int64
    bytes::SubArray{UInt8, 1, Vector{UInt8}, Tuple{UnitRange{Int64}}, true}
    sync::NTuple{16, UInt8}
end

StructTypes.StructType(::Type{Block}) = StructTypes.Struct()
const BlockType = schematype(Block)

"""
    Avro.writetable(io_or_file, tbl; kw...)

Write an input Tables.jl-compatible source table `tbl` out as an avro
object container file. `io_or_file` can be a file name as a `String` or
`IO` argument. If the input table supports `Table.partitions`, each
partition will be written as a separate "block" in the container file.

Because avro data is strictly typed, if the input table doesn't have a
well-defined schema (i.e. `Tables.schema(Tables.rows(tbl)) === nothing`),
then `Tables.dictrowtable(Tables.rows(tbl))` will be called, which scans
the input table, "building up" the schema based on types of values found
in each row.

Compression is supported via the `compress` keyword argument, and can
currently be one of `:zstd`, `:deflate`, `:bzip2`, or `:xz`.
"""
function writetable end

writetable(io_or_file; kw...) = x -> writetable(io_or_file, x; kw...)

function writetable(file::String, tbl; kw...)
    open(file, "w") do io
        writetable(io, tbl; kw...)
    end
    return file
end

function writetable(io::IO, source; compress::Union{Nothing, Symbol}=nothing, kw...)
    comp = get(COMPRESSORS, compress, nothing)
    parts = Tables.partitions(source)
    state = iterate(parts)
    state === nothing && error("no data in input; unable to write avro file")
    part, st = state
    rows = Tables.rows(part)
    sch = Tables.schema(rows)
    dictrow = false
    if sch === nothing || !Base.haslength(rows)
        rows = Tables.dictrowtable(rows)
        sch = Tables.schema(rows)
        dictrow = true
    end
    writewithschema(io, parts, rows, st, sch, dictrow, comp, kw)
    return io
end

function writewithschema(io, parts, rows, st, sch, dictrow, comp, kw)
    schtyp = schematype(sch)
    meta = Dict("avro.schema" => JSON3.write(schtyp))
    sync = _cast(NTuple{16, UInt8}, rand(UInt128))
    buf = write((magic=MAGIC, meta=meta, sync=sync); schema=FileHeaderRecordType)
    Base.write(io, buf)
    @debug 1 "wrote file header from bytes 1:$(pos - 1)"
    i = 1
    while true
        # if rows didn't have schema or length, we materialized w/ Tables.dictrowtable
        nrow = length(rows)
        @debug 1 "writing block count ($nrow) at pos = $pos"
        rowsstate = iterate(rows)
        pos = 1
        if rowsstate === nothing
            bytes = UInt8[]
            pos = 0
        else
            row, rowst = rowsstate
            # calc nbytes on first row, then allocate bytes
            bytesperrow = nbytes(schtyp, row)
            blen = trunc(Int, nrow * bytesperrow * 1.05) # add 5% cushion
            bytes = Vector{UInt8}(undef, blen)
            n = 1
            while true
                pos = writevalue(Binary(), schtyp, row, bytes, pos, blen, kw)
                rowsstate = iterate(rows, rowst)
                rowsstate === nothing && break
                row, rowst = rowsstate
                nb = nbytes(schtyp, row)
                if (pos + nb - 1) > blen
                    # unlikely, but need to resize our buffer
                    rowslefttowrite = nrow - n
                    avgbytesperrowwritten = div(bytesperrow, n)
                    resize!(bytes, pos + (rowslefttowrite * avgbytesperrowwritten))
                end
                bytesperrow += nb
                n += 1
            end
        end
        # compress
        if comp !== nothing
            finalbytes = transcode(comp[Threads.threadid()], unsafe_wrap(Base.Array, pointer(bytes), pos - 1))
        else
            finalbytes = bytes
        end
        block = Block(nrow, view(bytes, 1:(pos - 1)), sync)
        buf = write(block; schema=BlockType)
        Base.write(io, buf)
        state = iterate(parts, st)
        state === nothing && break
        part, st = state
        rows = Tables.rows(part)
        sch = Tables.schema(rows)
        if dictrow
            rows = Tables.dictrowtable(rows)
        end
    end
    return
end

"""
    Avro.Table

A Tables.jl-compatible source returned from `Avro.readtable`.
Conceptually, it can be thought of as an `AbstractVector{Record}`,
where `Record` is the avro version of a "row" or NamedTuple. Thus,
`Avro.Table` supports indexing/iteration like an `AbstractVector`.
"""
struct Table{T} <: AbstractVector{T}
    sch::Tables.Schema
    data::ChainedVector{T}
end

data(x::Table) = getfield(x, :data)
Base.IndexStyle(::Type{Table}) = Base.IndexLinear()
Base.@propagate_inbounds Base.getindex(x::Table, i::Int) = data(x)[i]
Base.size(x::Table) = size(data(x))
Base.IteratorSize(::Type{Table}) = Base.HasLength()
Base.IteratorEltype(::Type{Table}) = Base.HasEltype()
Base.iterate(x::Table) = iterate(data(x))
Base.iterate(x::Table, st) = iterate(data(x), st)

Tables.isrowtable(::Type{Table}) = true
Tables.schema(x::Table) = getfield(x, :sch)

"""
    Avro.readtable(file_or_io) => Avro.Table

Read an avro object container file, returning an [`Avro.Table`](@ref) type,
which is like an array of records, where each record follows the 
schema encoded with the file. Any compression will be detected and
decompressed automatically when reading.
"""
function readtable end

readtable(io::IO; kw...) = readtable(Base.read(io), 1, kw)
readtable(io::IOBuffer; kw...) = readtable(take!(io), 1, kw)
readtable(file::String; kw...) = readtable(Mmap.mmap(file), 1, kw)

function readtable(buf, pos, kw)
    header, pos = readvalue(Binary(), FileHeaderRecordType, FileHeader, buf, pos, length(buf), nothing)
    sch = JSON3.read(header.meta["avro.schema"], Schema)
    comp = get(header.meta, "avro.codec", nothing)
    comp = get(DECOMPRESSORS, Symbol(comp), nothing)
    T = juliatype(sch)
    data = readwithschema(T, sch, buf, pos, comp)
    return Table(Tables.Schema(T), data)
end

function readwithschema(::Type{T}, sch, buf, pos, comp) where {T}
    len = length(buf)
    blocks = Array{T}[]
    while pos <= len
        block, pos = readvalue(Binary(), BlockType, Block, buf, pos, len, nothing)
        @debug 1 "reading block: count = $(block.count), size = $(length(block.bytes))"
        values = Vector{Int}(undef, block.count)
        bytes = block.bytes
        # uncompress
        if comp !== nothing
            bytes = transcode(comp, bytes)
        end
        bpos = 1
        blen = length(bytes)
        for i = 1:block.count
            @inbounds values[i] = bpos
            bpos = skipvalue(Binary(), sch, T, bytes, bpos, blen, nothing)
        end
        push!(blocks, Array{T}(bytes, values))
    end
    return ChainedVector(blocks)
end
