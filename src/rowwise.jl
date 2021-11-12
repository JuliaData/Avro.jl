mutable struct recordwriter
    io :: IO
    schtyp :: SchemaType
    sync :: NTuple{16, UInt8}

    function recordwriter(schema::Tables.Schema, pth:: String)
        io = open(pth,"w")
        schtyp = schematype(schema)
        writeheader(io, schtyp)
        sync = _cast(NTuple{16, UInt8}, rand(UInt128))
        return new(io, schtyp, sync)
    end
end

function recordwriter(f::Function, schema::Tables.Schema, pth:: String)
    writer = recordwriter(schema, pth)
    try
        f(writer)
    finally
        close(writer)
    end
end

function writeheader(io::IO, schtyp::SchemaType)
    meta = Dict("avro.schema" => JSON3.write(schtyp))
    sync = _cast(NTuple{16, UInt8}, rand(UInt128))
    buf = write((magic=MAGIC, meta=meta, sync=sync); schema=FileHeaderRecordType)
    Base.write(io, buf)
end

function writerecord(arr::recordwriter, record)
    nb = nbytes(arr.schtyp, record)
    bytes = Vector{UInt8}(undef, nb)
    writevalue(Binary(), arr.schtyp, record, bytes, 1, nb, Dict())
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
        return new(buf, pos, pos, T, schtype, len, fh, comp)
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

function recordreader(f::Function, pth:: String)
    reader = recordreader(pth)
    try
        f(reader)
    finally
        close(reader)
    end
end

function close(arr::recordreader)
    close(arr.io)
end
