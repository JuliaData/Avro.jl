mutable struct recordwriter
    io :: IO
    schtyp :: SchemaType
    sync :: NTuple{16, UInt8}
    comp
end


"""
recordwriter(schema::Tables.Schema, pth:: String; compress::Union{Nothing, Symbol}=nothing)

Opens file handle at `pth` and writes `schema` as header. 
To write records as separate partitions use `writerecord(arr::recordwriter, record)` 
and call close(arr::recordwriter) to close file handle.

Compression is supported via the `compress` keyword argument, and can
currently be one of `:zstd`, `:deflate`, `:bzip2`, or `:xz`.

```julia
mktempdir() do p
pth = joinpath(p,"tmp.avro")
sch = Tables.Schema([:a, :b, :c, :d], 
    [Union{Missing, Int64}, Float64, Union{Missing, Int64}, Union{Int64, String}])
rw = Avro.recordwriter(sch, pth) 
Avro.writerecord(rw, (a=missing, b=2., c=3, d=4))
Avro.writerecord(rw, (a=1, b=4.0, c=missing, d=5))
Avro.writerecord(rw, (a=3, b=3.7, c=3, d="5"))
close(rw)
end
```
"""
function recordwriter(schema::Tables.Schema, pth:: String; compress::Union{Nothing, Symbol}=nothing)
    io = open(pth,"w")
    schtyp = schematype(schema)
    sync = _cast(NTuple{16, UInt8}, rand(UInt128))
    comp = get(COMPRESSORS, compress, nothing)
    meta = Dict("avro.schema" => JSON3.write(schtyp))
    if comp !== nothing
        meta["avro.codec"] = String(compress)
    end
    sync = _cast(NTuple{16, UInt8}, rand(UInt128))
    buf = write((magic=MAGIC, meta=meta, sync=sync); schema=FileHeaderRecordType)
    Base.write(io, buf)
    return recordwriter(io, schtyp, sync, comp)
end

"""
recordwriter(f::Function, schema::Tables.Schema, pth:: String; compress::Union{Nothing, Symbol}=nothing)

Writes `schema` as header to `pth` and ensures file handle is closed.
To write records as separate partitions use `writerecord(arr::recordwriter, record)`.

Compression is supported via the `compress` keyword argument, and can
currently be one of `:zstd`, `:deflate`, `:bzip2`, or `:xz`.

```julia
mktempdir() do p
    pth = joinpath(p,"tmp.avro")
    sch = Tables.Schema([:a, :b, :c, :d], 
        [Union{Missing, Int64}, Float64, Union{Missing, Int64}, Union{Int64, String}])
    Avro.recordwriter(sch, pth) do rw
        Avro.writerecord(rw, (a=missing, b=2., c=3, d=4))
        Avro.writerecord(rw, (a=1, b=4.0, c=missing, d=5))
        Avro.writerecord(rw, (a=3, b=3.7, c=3, d="5"))
    end
end
```
"""
function recordwriter(f::Function, schema::Tables.Schema, pth:: String; kw...)
    writer = recordwriter(schema, pth; kw...)
    try
        f(writer)
    finally
        close(writer)
    end
end

function writerecord(arr::recordwriter, record)
    nb = nbytes(arr.schtyp, record)
    bytes = Vector{UInt8}(undef, nb)
    posrecord = writevalue(Binary(), arr.schtyp, record, bytes, 1, nb, Dict())
    if arr.comp !== nothing
        bytes = transcode(arr.comp[Threads.threadid()], unsafe_wrap(Base.Array, pointer(bytes), posrecord - 1))
    end
    block = Block(1, view(bytes, 1:length(bytes)), arr.sync)
    Base.write(arr.io, write(block; schema=BlockType))
end

function close(arr::recordwriter)
    close(arr.io)
end

mutable struct recordreader
    buf:: Vector{UInt8}
    pos:: Int
    firstrecordpos:: Int
    T :: DataType
    schtype :: RecordType
    len :: Int
    io :: IO
    comp
end

"""
recordreader(pth:: String)

Open avro file and read header. Use `iterate` to access one partition at a time.
Close filehandle with `close(arr::recordreader)`

Any compression will be detected and decompressed automatically when reading.

```julia
mktempdir() do p
    pth = joinpath(p,"tmp.avro")
    sch = Tables.Schema([:a, :b], [Int, Int])
    Avro.recordwriter(sch, pth) do rw
        Avro.writerecord(rw, (a=1, b=2))
        Avro.writerecord(rw, (a=1, b=4))
    end
    rr = Avro.recordreader(pth)
    @assert NamedTuple(first(rr)) == (a=1, b=2)
    close(rr)
end
```
"""
function recordreader(pth:: String)
    fh = open(pth)
    buf = Mmap.mmap(fh)
    header, pos = readvalue(Binary(), FileHeaderRecordType, FileHeader, buf, 1, length(buf), nothing)
    sch = JSON3.read(header.meta["avro.schema"], Schema)
    comp = get(header.meta, "avro.codec", nothing)
    comp = get(DECOMPRESSORS, Symbol(comp), nothing)
    T = juliatype(sch)
    len = length(buf)
    schtype = schematype(T)
    return recordreader(buf, pos, pos, T, schtype, len, fh, comp)
end

"""
    recordreader(f::Function, pth:: String)

Open avro file and read header. Use `iterate` to access one partition at a time.

Any compression will be detected and decompressed automatically when reading.

```julia
mktempdir() do p
    pth = joinpath(p,"tmp.avro")
    sch = Tables.Schema([:a, :b], [Int, Int])
    Avro.recordwriter(sch, pth) do rw
        Avro.writerecord(rw, (a=1, b=2))
        Avro.writerecord(rw, (a=1, b=4))
    end
    Avro.recordreader(pth) do rr
        @assert NamedTuple(first(rr)) == (a=1, b=2)
    end
end
```
"""
function recordreader(f::Function, pth:: String)
    reader = recordreader(pth)
    try
        f(reader)
    finally
        close(reader)
    end
end

function iterate(r::recordreader, state=nothing)
    if isnothing(state) 
        r.pos = r.firstrecordpos 
    end
    try
        read(r), r
    catch e
        if isa(e, EOFError)
            return nothing
        else
            throw(e)
        end
    end
end

function read(reader::recordreader)
    reader.pos >= reader.len && throw(EOFError())
    block, pos = readvalue(Binary(), BlockType, Block, reader.buf, reader.pos, reader.len, nothing)
    reader.pos = pos
    @debug 1 "reading block: count = $(block.count), size = $(length(block.bytes))"
    bytes = block.bytes
    # uncompress
    if reader.comp !== nothing
        bytes = transcode(comp, Vector{UInt8}(bytes))
    end
    v, _ = readvalue(Binary(), reader.schtype, reader.T, bytes, 1, length(bytes), nothing)
    return v
end

function close(arr::recordreader)
    close(arr.io)
end
