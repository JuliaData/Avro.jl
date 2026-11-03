# Object container files: a block reader over memory-mapped, byte and streamed sources, a writer with
# atomic publication and failure poisoning, `Avro.write`/`Avro.tobuffer` over Tables.jl sources, and
# the permissive `Avro.inspect` diagnostic.

import Mmap

const MAGIC = (UInt8('O'), UInt8('b'), UInt8('j'), 0x01)

# ---- sources ----------------------------------------------------------------------------------------

mutable struct BytesSource{B<:AbstractVector{UInt8}}
    const buf::B
    pos::Int
    const stop::Int
    const mapped::Bool       # `buf` is this source's own memory map
end

function BytesSource(buf::AbstractVector{UInt8}, pos::Int, stop::Int=length(buf))
    return BytesSource(buf, pos, stop, false)
end

mutable struct StreamSource
    const io::IO
    const owned::Bool
    pos::Int
end

function StreamSource(io::IO, owned::Bool)
    return StreamSource(io, owned, 1)
end

const BlockSource = Union{BytesSource,StreamSource}

function sourceeof(s::BytesSource)
    return s.pos > s.stop
end

function sourceeof(s::StreamSource)
    return eof(s.io)
end

function sourcebyte(s::BytesSource)
    s.pos <= s.stop || throw(DataError("truncated file", s.pos))
    b = s.buf[s.pos]
    s.pos += 1
    return b
end

function sourcebyte(s::StreamSource)
    eof(s.io) && throw(DataError("truncated file", s.pos))
    byte = Base.read(s.io, UInt8)
    s.pos += 1
    return byte
end

"A zig-zag varint long read byte-wise from the source (the container's counts and sizes)."
function sourcevarint(s::BlockSource)
    v = UInt64(0)
    shift = 0
    while true
        b = sourcebyte(s)
        shift == 63 && (b & 0xfe) != 0 && throw(DataError("varint overflows 64 bits", position(s)))
        v |= UInt64(b & 0x7f) << shift
        (b & 0x80) == 0 && break
        shift += 7
        shift > 63 && throw(DataError("varint longer than 10 bytes", position(s)))
    end
    return reinterpret(Int64, (v >> 1) ⊻ (~(v & 1) + 1))
end

function Base.position(s::BytesSource)
    return s.pos
end

function Base.position(s::StreamSource)
    return s.pos
end

"Exactly `n` payload bytes: a view for byte sources, an owned buffer for streams."
function sourcepayload(s::BytesSource, n::Int)
    n <= s.stop - s.pos + 1 || throw(DataError("truncated file", s.pos))
    out = view(s.buf, s.pos:s.pos + n - 1)
    s.pos += n
    return out
end

function sourcepayload(s::StreamSource, n::Int)
    out = Vector{UInt8}(undef, n)
    consumed = readbytes!(s.io, out, n)
    s.pos += consumed
    consumed == n || throw(DataError("truncated file", s.pos))
    return out
end

"Exactly `n` payload bytes as a caller-owned vector."
function ownedpayload(s::BlockSource, n::Int)
    payload = sourcepayload(s, n)
    return payload isa Vector{UInt8} ? payload : Vector{UInt8}(payload)
end

function checksync(s::BlockSource, sync::NTuple{16,UInt8}, blockindex::Int)
    for i in 1:16
        sourcebyte(s) == sync[i] || throw(DataError("sync marker mismatch after block $blockindex", position(s)))
    end
    return nothing
end

# Decoded values never alias a source's bytes, so a map is released now rather than at some later collection:
# maps are a scarce OS resource, and on Windows one blocks replacing or truncating its file.
function closesource(s::BytesSource)
    s.mapped && finalize(s.buf)
    return nothing
end

function closesource(s::StreamSource)
    return s.owned ? close(s.io) : nothing
end

function opensource(src::Vector{UInt8}; mmap::Bool=true)
    return BytesSource(src, 1)
end

function opensource(src::AbstractVector{UInt8}; mmap::Bool=true)
    return BytesSource(copyto!(Vector{UInt8}(undef, length(src)), src), 1)
end

function opensource(src::IOBuffer; mmap::Bool=true)
    return BytesSource(src.data, iobufferstart(src), src.size)
end

# On Julia 1.11+ an IOBuffer over a Vector with a memory offset starts after index 1 of `data`.
function iobufferstart(src::IOBuffer)
    return max(1, src.ptr - position(src))
end

function opensource(src::IO; mmap::Bool=true)
    return StreamSource(src, false)
end

function opensource(src::AbstractString; mmap::Bool=true)
    mmap || return StreamSource(open(src, "r"), true)
    map = open(io -> Mmap.mmap(io, Vector{UInt8}), src, "r")
    return BytesSource(map, 1, length(map), true)
end

# ---- header -----------------------------------------------------------------------------------------

"One metadata entry: an owned UTF-8 key and value, with the running byte total kept under `max_bytes`."
function readmetadataentry(s::BlockSource, limits::Limits, total::Int)
    klen = sourcevarint(s)
    klen >= 0 || throw(DataError("negative metadata key length $klen", position(s)))
    klen <= limits.max_bytes || throw(LimitError(:max_bytes, klen, limits.max_bytes))
    kbytes = ownedpayload(s, Int(klen))
    isstrictutf8(kbytes) || throw(DataError("metadata key is not valid UTF-8", position(s)))
    vlen = sourcevarint(s)
    vlen >= 0 || throw(DataError("negative metadata value length $vlen", position(s)))
    vlen <= limits.max_bytes || throw(LimitError(:max_bytes, vlen, limits.max_bytes))
    total = checked_add(checked_add(total, Int(klen)), Int(vlen))
    total <= limits.max_bytes || throw(LimitError(:max_bytes, total, limits.max_bytes))
    return (String(kbytes), ownedpayload(s, Int(vlen)), total)
end

"The header's metadata map (duplicate keys rejected; sized blocks must be consumed exactly)."
function readmetadata(s::BlockSource, limits::Limits)
    metadata = Dict{String,Vector{UInt8}}()
    total = 0
    while true
        count = sourcevarint(s)
        count == 0 && break
        size = -1
        if count < 0
            count == typemin(Int64) && throw(DataError("metadata block count typemin(Int64)", position(s)))
            count = -count
            size = sourcevarint(s)
            size >= 0 || throw(DataError("negative metadata block size $size", position(s)))
            size <= limits.max_bytes || throw(LimitError(:max_bytes, size, limits.max_bytes))
        end
        blockstart = position(s)
        for _ in 1:count
            key, value, total = readmetadataentry(s, limits, total)
            haskey(metadata, key) && throw(DataError("duplicate metadata key \"$(diagnostictext(key))\"", position(s)))
            metadata[key] = value
        end
        if size >= 0
            consumed = position(s) - blockstart
            consumed == size || throw(DataError("metadata block declares $size bytes but its entries consume $consumed", position(s)))
        end
    end
    return metadata
end

"The `avro.codec` name of a parsed header (\"null\" when the entry is absent)."
function headercodecname(metadata::Dict{String,Vector{UInt8}}, s::BlockSource)
    codecbytes = get(metadata, "avro.codec", nothing)
    codecbytes === nothing && return "null"
    isstrictutf8(codecbytes) || throw(DataError("avro.codec is not valid UTF-8", position(s)))
    return String(copy(codecbytes))
end

"The parsed container header: metadata, schema, codec name and sync marker."
function readheader(s::BlockSource, limits::Limits; legacy, allow_invalid_names::Bool, allow_invalid_defaults::Bool)
    for m in MAGIC
        (sourceeof(s) || sourcebyte(s) != m) && throw(DataError("not an Avro object container file (bad magic)", position(s)))
    end
    metadata = readmetadata(s, limits)
    sync = ntuple(_ -> sourcebyte(s), 16)
    schemabytes = get(metadata, "avro.schema", nothing)
    schemabytes === nothing && throw(DataError("the container has no avro.schema", position(s)))
    schema = parseschemaimpl(schemabytes, legacy === :avrojl1; allow_invalid_names=allow_invalid_names,
                             allow_invalid_defaults=allow_invalid_defaults, limits=limits)
    return (metadata=metadata, schema=schema, codecname=headercodecname(metadata, s), sync=sync)
end

# ---- Reader -----------------------------------------------------------------------------------------

"""
    Avro.Reader(src; limits=Limits(), legacy=nothing, allow_invalid_names=false, allow_invalid_defaults=false,
                mmap=true)

A block-level container reader over a file path (memory-mapped by default; `mmap=false` streams), a byte
vector (caller-owned), an `IOBuffer` (its written bytes) or a streaming `IO`. `eachblock(r)` yields
`(count, bytes)` with the owned decompressed block; `eachdatum(r)` yields the schema's generic values;
`close(r)` is idempotent (the do-block form closes on exit; a finalizer closes a path-owned source as a
fallback). `legacy=:avrojl1` tolerates the Avro.jl ≤ 1.1.2 container defects (padded null-codec blocks, the
`zstd` codec name, unnamed fixed schemas) and reads its little-endian decimals.
"""
mutable struct Reader
    source::Union{Nothing,BlockSource}
    const schema::Schema
    const plan::Plan
    const codecname::Symbol
    const metadata::Dict{String,Vector{UInt8}}
    const sync::NTuple{16,UInt8}
    const limits::Limits
    const legacy::Union{Nothing,Symbol}
    blockindex::Int
    closed::Bool
    warned::Bool
    failed::Bool                # set while reading a block's framing; left set by an error, since the position is then unknown
end

"Close a reader's source. Explicit close reports the source failure; `suppress=true` swallows it."
function closereaderresources(source; suppress::Bool=false)
    try
        source === nothing || closesource(source)
    catch err
        suppress || rethrow()
    end
    return nothing
end

function Reader(src; limits::Limits=Limits(), legacy::Union{Nothing,Symbol}=nothing,
                allow_invalid_names::Bool=false, allow_invalid_defaults::Bool=false, mmap::Bool=true)
    legacy in (nothing, :avrojl1) || throw(ArgumentError("legacy must be nothing or :avrojl1"))
    source = opensource(src; mmap=mmap)
    r = try
        h = readheader(source, limits; legacy=legacy, allow_invalid_names=allow_invalid_names, allow_invalid_defaults=allow_invalid_defaults)
        Reader(source, h.schema, readplan(h.schema; mode=legacy === :avrojl1 ? :little : :standard), readercodec(h.codecname, legacy), h.metadata, h.sync,
               limits, legacy, 0, false, false, false)
    catch
        closereaderresources(source; suppress=true)
        rethrow()
    end
    finalizer(r) do x
        if !x.closed
            source = x.source
            x.source = nothing
            x.closed = true
            closereaderresources(source; suppress=true)
        end
        return nothing
    end
    return r
end

function Base.show(io::IO, r::Reader)
    return print(io, "Avro.Reader(", schemalabel(r.schema), ", codec=", r.codecname, r.source === nothing ? ", closed)" : ")")
end

function Reader(f::Function, src; kw...)
    r = Reader(src; kw...)
    try
        return f(r)
    finally
        close(r)
    end
end

function Base.close(r::Reader)
    r.closed && return nothing
    source = r.source
    r.source = nothing
    r.closed = true
    return closereaderresources(source)
end

function checkopen(r::Reader)
    r.closed && throw(ArgumentError("the reader is closed"))
    r.failed && throw(ArgumentError("an earlier error ended this stream"))
    return nothing
end

"""
    Avro.metadata(r) -> Dict{String,Vector{UInt8}}; Avro.codec(r) -> Symbol; Avro.sync(r) -> NTuple{16,UInt8}
    Avro.writerschema(r) -> Schema; Avro.schema(r) -> Schema
"""
function metadata(r::Reader)
    return r.metadata
end

function codec(r::Reader)
    return r.codecname
end

function sync(r::Reader)
    return r.sync
end

function writerschema(r::Reader)
    return r.schema
end

function schema(r::Reader)
    return r.schema
end

# Avro.jl ≤ 1.1.2 sized null-codec blocks with a cushion, so a legacy block may carry bytes after its
# declared datums: walk them to find where they end, warn once and drop the padding.
function exactblock(r::Reader, bytes::Vector{UInt8}, count::Int, index::Int)
    r.legacy === :avrojl1 || return bytes
    d = Decoder(bytes, r.limits)
    for _ in 1:count
        skip(r.plan, d)
    end
    d.pos == length(bytes) + 1 && return bytes
    r.codecname === :null ||
        throw(DataError("block $index declares $count datums but they consume $(d.pos - 1) of $(length(bytes)) bytes", d.pos))
    r.warned ||
        (@warn "accepting trailing bytes after $count datums in a null-codec block (legacy=:avrojl1; Avro.jl ≤ 1.1.2 sizing cushion)" source = 1; r.warned = true)
    return resize!(bytes, d.pos - 1)
end

"Read the next block as `(count, bytes)` with the owned decompressed bytes; `nothing` at a clean end of the file."
function nextblock!(r::Reader)
    checkopen(r)
    source = r.source::BlockSource
    sourceeof(source) && return nothing
    r.failed = true
    count = sourcevarint(source)
    count >= 0 || throw(DataError("negative block count $count", position(source)))
    size = sourcevarint(source)
    size >= 0 || throw(DataError("negative block size $size", position(source)))
    size <= r.limits.max_bytes || throw(LimitError(:max_bytes, size, r.limits.max_bytes))
    r.blockindex += 1
    payload = sourcepayload(source, Int(size))
    checksync(source, r.sync, r.blockindex)
    r.failed = false                                   # a bad payload below leaves the next block readable
    bytes = exactblock(r, decompressblock(r.codecname, payload, r.limits), Int(count), r.blockindex)
    count == 0 && !isempty(bytes) && throw(DataError("a zero-count block has a non-empty payload", 1))
    return (Int(count), bytes)
end

function checkconsumed(d::Decoder)
    d.pos == d.stop + 1 || throw(DataError("block datums did not consume the block exactly", d.pos))
    return nothing
end

struct EachBlock
    reader::Reader
end

"""
    Avro.eachblock(r::Reader)

Iterate `(count, bytes)` pairs, `bytes` being the owned decompressed block.
"""
function eachblock(r::Reader)
    return EachBlock(r)
end

function Base.IteratorSize(::Type{EachBlock})
    return Base.SizeUnknown()
end

function Base.eltype(::Type{EachBlock})
    return Tuple{Int,Vector{UInt8}}
end

function Base.iterate(it::EachBlock, ::Nothing=nothing)
    block = nextblock!(it.reader)
    block === nothing && return nothing
    return (block, nothing)
end

mutable struct EachDatum
    const reader::Reader
    const decoder::Decoder{Vector{UInt8}}
    remaining::Int
end

"""
    Avro.eachdatum(r::Reader)

Iterate the file's datums as the schema's generic values (any root schema).
"""
function eachdatum(r::Reader)
    return EachDatum(r, Decoder(UInt8[], r.limits), 0)
end

function Base.IteratorSize(::Type{EachDatum})
    return Base.SizeUnknown()
end

function Base.iterate(it::EachDatum, ::Nothing=nothing)
    v = nextdatum!(d -> decode(it.reader.plan, d), it)
    return v === nothing ? nothing : (something(v), nothing)
end

# The next datum of a datum cursor (`EachDatum`, `Rows`: a `reader`, its block `decoder`, and the datums
# `remaining` in the current block) decoded by `decodeone`, or `nothing` at a clean end. A datum that
# fails takes the rest of its block with it, so iterating again continues with the next block.
function nextdatum!(decodeone::F, it) where {F}
    checkopen(it.reader)
    d = it.decoder
    while it.remaining == 0
        blk = nextblock!(it.reader)
        blk === nothing && return nothing
        it.remaining = blk[1]
        reset!(d, blk[2], 1)
        it.remaining == 0 && checkconsumed(d)
    end
    left = it.remaining - 1
    it.remaining = 0
    v = decodeone(d)
    left == 0 && checkconsumed(d)
    it.remaining = left
    return Some(v)
end

# ---- Writer -----------------------------------------------------------------------------------------

function writevarint(io::IO, n::Integer)
    v = (reinterpret(UInt64, Int64(n)) << 1) ⊻ reinterpret(UInt64, Int64(n) >> 63)
    while true
        b = UInt8(v & 0x7f)
        v >>= 7
        if v == 0
            Base.write(io, b)
            break
        end
        Base.write(io, b | 0x80)
    end
    return nothing
end

"""
    varintlength(n) -> Int

The number of bytes of the zig-zag varint encoding of the non-negative integer `n`.
"""
function varintlength(n::Integer)
    v = UInt64(n) << 1
    len = 1
    while v >= 0x80
        v >>= 7
        len += 1
    end
    return len
end

"""
    Avro.Writer(dst, schema; codec=nothing, level=nothing, metadata=Dict(), sync=nothing, block_bytes=64*1024,
                atomic=true, append=false, fsync=false, allow_invalid_names=false, allow_invalid_defaults=false,
                limits=Limits())

A container writer to a path (atomic by default: a sibling temp file renamed into place on `close`) or a
caller-owned `IO` (flushed, never closed). The path's parent directory, including parent symbolic links,
is resolved when the writer opens. Changing the working directory later does not redirect its output.
`codec` is `:null` (uncompressed, what `nothing` means for a new file), `:deflate` (`level` 0–9, default 6),
`:snappy`, `:zstandard` (`level` 1–22, default 3), or `:bzip2`/`:xz` once `CodecBzip2`/`CodecXz` is loaded.
Datums are encoded into the current block, which is emitted at `block_bytes`, before a datum would push
it past `max_bytes` or its value cap, or on `flush`/`close`; a datum that exceeds either bound on its own
is a `LimitError`. The first failure poisons the writer, including a rejected datum from `push!`;
`WriterClosedError` carries the original cause. With `atomic=true`, the temp file is removed and the
destination is untouched; `atomic=false` and caller-owned streams can retain partial output.
`close(w; abort=true)` discards the buffered block. The do-block form closes on success and aborts on
error; a finalizer aborts an unclosed writer without closing caller-owned I/O.

`append=true` adds blocks to the existing file at path `dst` in place, keeping its codec (`codec` must be
`nothing` or match it), metadata and sync marker (a missing or empty file is created as usual). `schema` must equal the file's schema and the file must
end at a block boundary. Aborting removes the blocks this writer added, unless the file has since grown past
them; a writer dropped without closing leaves the blocks it flushed. Readers see each block once it is flushed.
A crash can leave a torn last block, which the next append or read reports.
"""
mutable struct Writer
    const sink::IO
    const path::Union{Nothing,String}
    const temppath::Union{Nothing,String}
    const schema::Schema
    const plan::Plan
    const wcodec::WriterCodec
    const syncmarker::NTuple{16,UInt8}
    const limits::Limits
    const blockbytes::Int
    const fsync::Bool
    const ownsink::Bool
    const encoder::Encoder
    const datumencoder::Encoder             # scratch: a datum moving to the next block, diagnostic replays
    const appendstart::Int                  # the file's length before an append; -1 otherwise
    appendlength::Int                       # the file's length with every block this writer has emitted
    pendingcount::Int
    closed::Bool
    poison::Union{Nothing,Exception}
end

"The writer's 16-byte sync marker: validated caller bytes, or fresh `RandomDevice` bytes."
function writersyncmarker(sync)
    sync === nothing && return ntuple(_ -> rand(RandomDevice(), UInt8), 16)
    (sync isa AbstractVector{UInt8} && length(sync) == 16) || throw(ArgumentError("sync must be exactly 16 bytes"))
    return ntuple(i -> sync[i], 16)
end

"The header entries: the schema JSON, the codec name and the validated user metadata."
function writerheaderentries(schema::Schema, codec::Symbol, metadata, limits::Limits)
    schemajson = json(schema)
    parsejson(Vector{UInt8}(codeunits(schemajson)); limits=limits, errfn=(msg, pos) -> throw(ArgumentError(msg)))   # readable under these limits
    codecname = String(codec)
    entries = Tuple{String,Vector{UInt8}}[("avro.schema", Vector{UInt8}(codeunits(schemajson))),
                                          ("avro.codec", Vector{UInt8}(codeunits(codecname)))]
    total = checked_add(checked_add(11, sizeof(schemajson)), checked_add(10, sizeof(codecname)))
    total <= limits.max_bytes || throw(LimitError(:max_bytes, total, limits.max_bytes))
    for (k, v) in metadata
        k isa AbstractString && v isa Union{AbstractString,AbstractVector{UInt8}} || throw(ArgumentError("metadata must map strings to strings or byte vectors"))
        startswith(k, "avro.") && throw(ArgumentError("metadata keys in the avro.* namespace are reserved; avro.schema and avro.codec come from the constructor"))
        isstrictutf8(k) || throw(ArgumentError("metadata keys must be valid UTF-8"))
        value = v isa AbstractString ? Vector{UInt8}(codeunits(v)) : Vector{UInt8}(v)
        total = checked_add(total, checked_add(sizeof(k), length(value)))
        total <= limits.max_bytes || throw(LimitError(:max_bytes, total, limits.max_bytes))
        push!(entries, (String(k), value))
    end
    return entries
end

"Write the container magic, the metadata block and the sync marker to the sink."
function writecontainerheader!(sink, entries, syncmarker)
    for m in MAGIC
        Base.write(sink, m)
    end
    writevarint(sink, length(entries))
    for (k, v) in entries
        writevarint(sink, sizeof(k))
        Base.write(sink, codeunits(k))
        writevarint(sink, length(v))
        Base.write(sink, v)
    end
    Base.write(sink, 0x00)
    for b in syncmarker
        Base.write(sink, b)
    end
    return nothing
end

"The mode of a published file: the replaced file's mode, else the mode `open` gives a new file."
function publishmode(path::String)
    isfile(path) && return filemode(path) & 0o777
    Sys.iswindows() && return 0o666
    # Let file creation apply the umask without changing process-wide permissions while other tasks run.
    return mktempdir(dirname(path)) do dir
        open(joinpath(dir, "mode"), "w") do io
            return filemode(stat(io)) & 0o777
        end
    end
end

# The file a path names: its directory resolved now, and a symbolic link followed so that atomic replacement
# updates the file it points to.
function writerpath(dst::AbstractString)
    path = joinpath(realpath(joinpath(dirname(dst), ".")), basename(dst))
    return islink(path) && ispath(path) ? realpath(path) : path
end

"The writer's destination: `(sink, temppath, ownsink)`, a temp file first when writing atomically."
function openwritersink(dst::Union{AbstractString,IO}, atomic::Bool)
    path = dst isa AbstractString ? String(dst) : nothing
    path === nothing && return (dst, nothing, false)
    if atomic
        t, tio = mktemp(dirname(abspath(path)))
        return (tio, t, true)
    end
    return (open(path, "w"), nothing, true)
end

# The codec and sync marker of the container being appended to, after checking the writer's arguments
# against its header and that the file ends at a block boundary.
function appendheader(path::String, schema::Schema, codec, metadata, sync, limits::Limits, allow_invalid_names::Bool,
                      allow_invalid_defaults::Bool)
    (isempty(metadata) && sync === nothing) || throw(ArgumentError("append=true keeps the file's metadata and sync marker"))
    codecname, syncmarker = Reader(path; mmap=false, limits=limits, allow_invalid_names=allow_invalid_names,
                                   allow_invalid_defaults=allow_invalid_defaults) do r
        schemaequal(r.schema, schema) ||
            throw(ArgumentError("the schema differs from that of $path; append with the file's schema"))
        codec === nothing || codec === r.codecname ||
            throw(ArgumentError("$path uses the $(r.codecname) codec; append=true cannot write $codec blocks into it"))
        return (r.codecname, r.sync)
    end
    tail = open(io -> (seek(io, filesize(path) - 16); Base.read(io, 16)), path)
    Tuple(tail) == syncmarker || throw(DataError("$path does not end at a block boundary; its last block is torn", filesize(path) + 1))
    return (codecname, syncmarker)
end

function Writer(dst::Union{AbstractString,IO}, schema::Schema; codec::Union{Nothing,Symbol}=nothing, level=nothing,
                metadata=Dict{String,Vector{UInt8}}(), sync=nothing, block_bytes::Integer=64 * 1024,
                atomic::Bool=true, append::Bool=false, fsync::Bool=false, allow_invalid_names::Bool=false,
                allow_invalid_defaults::Bool=false, limits::Limits=Limits())
    block_bytes > 0 || throw(ArgumentError("block_bytes must be positive, got $block_bytes"))
    gi = graphinfo(schema)
    gi.repaired_names && !allow_invalid_names && throw(ArgumentError("the schema contains invalid names; pass allow_invalid_names=true to write it"))
    gi.repaired_defaults && !allow_invalid_defaults && throw(ArgumentError("the schema contains invalid defaults; pass allow_invalid_defaults=true to write it"))
    metadata isa AbstractDict || throw(ArgumentError("metadata must map strings to strings or byte vectors"))
    append && dst isa IO && throw(ArgumentError("append=true needs a file path"))
    path = dst isa AbstractString ? writerpath(dst) : nothing
    if path !== nothing && append && isfile(path) && filesize(path) > 0
        codecname, syncmarker = appendheader(path, schema, codec, metadata, sync, limits, allow_invalid_names, allow_invalid_defaults)
        wcodec = writercodec(codecname, level)
        entries = nothing
        appendstart = filesize(path)
        sink, temppath, ownsink = (open(path, "a"), nothing, true)
    else
        syncmarker = writersyncmarker(sync)
        wcodec = writercodec(something(codec, :null), level)
        entries = writerheaderentries(schema, something(codec, :null), metadata, limits)
        appendstart = -1
        sink, temppath, ownsink = openwritersink(path === nothing ? dst : path, atomic)
    end
    w = Writer(sink, path, atomic ? temppath : nothing, schema, writeplan(schema), wcodec, syncmarker, limits,
               min(Int(block_bytes), limits.max_bytes), fsync, ownsink, Encoder(limits), Encoder(limits), appendstart, appendstart, 0, false, nothing)
    try
        entries === nothing || writecontainerheader!(sink, entries, syncmarker)
    catch e
        poison!(w, e)
        abortcleanup(w; suppress=true)
        rethrow()
    end
    finalizer(w) do x                                # the blocks of an unclosed append stay, as a crash would leave them
        if !x.closed
            x.closed = true
            cleanupwritersink(x.sink, x.temppath, x.ownsink; suppress=true)
        end
        return nothing
    end
    return w
end

function Base.show(io::IO, w::Writer)
    return print(io, "Avro.Writer(", schemalabel(w.schema), ", codec=", w.wcodec.name, w.closed ? ", closed)" : ")")
end

function Writer(f::Function, dst, schema::Schema; kw...)
    w = Writer(dst, schema; kw...)
    try
        result = f(w)
        close(w)
        return result
    catch err
        try
            close(w; abort=true)
        catch cleanup
            throw(CompositeException(Any[err, cleanup]))
        end
        rethrow()
    end
end

function poison!(w::Writer, e::Exception)
    return (w.poison === nothing && (w.poison = e); nothing)
end

function checkwritable(w::Writer)
    w.closed && throw(WriterClosedError(w.poison))
    w.poison === nothing || throw(WriterClosedError(w.poison))
    return nothing
end

function cleanupwritersink(sink, temppath, ownsink::Bool; suppress::Bool=false)
    failure = nothing
    try
        ownsink && close(sink)
    catch err
        failure = err
    end
    try
        temppath !== nothing && rm(temppath; force=true)
    catch err
        failure === nothing && (failure = err)
    end
    suppress || failure === nothing || throw(failure)
    return nothing
end

# An aborted append removes its own blocks, unless the file has since grown past them: what follows them is
# another writer's.
function abortcleanup(w::Writer; suppress::Bool=false)
    if w.appendstart >= 0
        try
            flush(w.sink)
            filesize(w.path) <= w.appendlength && Base.truncate(w.sink, w.appendstart)
        catch
            suppress || rethrow()
        end
    end
    return cleanupwritersink(w.sink, w.temppath, w.ownsink; suppress=suppress)
end

function Base.push!(w::Writer, datum)
    checkwritable(w)
    try
        return pushdatum!(w, datum)
    catch e
        poison!(w, e)
        rethrow()
    end
end

function pushdatum!(w::Writer, datum)
    plans = alignedplans(w.plan, typeof(datum))
    plans === nothing && return pushgenericdatum!(w, datum)
    return pushaligneddatum!(w, datum, plans)
end

"Encode one datum in place at the end of the current block; a failed datum leaves no bytes behind."
function pushgenericdatum!(w::Writer, datum)
    e = w.encoder
    start = e.pos
    values = e.values
    try
        encodedatum!(w.plan, e, datum, w.schema)
    catch
        e.pos = start
        e.values = values
        rethrow()
    end
    return commitdatum!(w, start, values)
end

"One aligned row (a NamedTuple or struct with its plan tuple, or a `CellRow` with its field cells) through the fast path."
@inline function pushaligneddatum!(w::Writer, datum, plans)
    e = w.encoder
    start = e.pos
    values = e.values
    try
        encodealignedfast!(plans, e, datum)
    catch err
        e.pos = start
        e.values = values
        err isa EncodeError && replayaligned!(w.plan, w.datumencoder, datum isa CellRow ? cellrecord(plans, datum, w.schema::RecordSchema) : datum, w.schema::RecordSchema)
        rethrow()
    end
    return commitdatum!(w, start, values)
end

"Whether a block of `bytes` bytes holding `values` values exceeds what a reader with these limits accepts."
function blockexceeds(limits::Limits, bytes::Int, values::Int)
    return bytes > limits.max_bytes || values > valuecap(limits, bytes)
end

"Account the datum encoded since `start`: move it to the next block when it overflows this one, then flush at `block_bytes`."
function commitdatum!(w::Writer, start::Int, values::Int)
    e = w.encoder
    if w.pendingcount > 0 && blockexceeds(w.limits, e.pos, e.values)
        datumbytes = e.pos - start
        datumvalues = e.values - values
        scratch = w.datumencoder
        writeraw!(scratch, e.buf, start + 1, datumbytes)
        e.pos = start
        e.values = values
        try
            flushblock!(w)
            writeraw!(e, scratch.buf, 1, datumbytes)
        finally
            reset!(scratch)
        end
        e.values = datumvalues
    end
    if blockexceeds(w.limits, e.pos, e.values)
        e.pos > w.limits.max_bytes && throw(LimitError(:max_bytes, e.pos, w.limits.max_bytes))
        throw(LimitError(:max_values_per_byte, e.values, valuecap(w.limits, e.pos)))
    end
    w.pendingcount += 1
    e.pos >= w.blockbytes && flushblock!(w)
    return w
end

function flushblock!(w::Writer)
    w.pendingcount == 0 && return nothing
    try
        compressed = compressblock(w.wcodec, take!(w.encoder))
        length(compressed) <= w.limits.max_bytes || throw(LimitError(:max_bytes, length(compressed), w.limits.max_bytes))
        w.appendlength += varintlength(w.pendingcount) + varintlength(length(compressed)) + length(compressed) + 16
        writevarint(w.sink, w.pendingcount)
        writevarint(w.sink, length(compressed))
        Base.write(w.sink, compressed)
        for b in w.syncmarker
            Base.write(w.sink, b)
        end
    catch e
        poison!(w, e)
        rethrow()
    end
    w.pendingcount = 0
    return nothing
end

function Base.flush(w::Writer)
    checkwritable(w)
    flushblock!(w)
    try
        flush(w.sink)
    catch e
        poison!(w, e)
        rethrow()
    end
    return nothing
end

function syncfile(io::IO)
    flush(io)
    io isa IOStream || return nothing
    @static if Sys.iswindows()
        result = ccall(:_commit, Cint, (Cint,), fd(io))
    else
        result = ccall(:fsync, Cint, (Cint,), fd(io))
    end
    Base.systemerror("syncing Avro output", result != 0)
    return nothing
end

function Base.close(w::Writer; abort::Bool=false)
    w.closed && return nothing
    if abort || w.poison !== nothing
        w.closed = true
        abortcleanup(w)
        return nothing
    end
    try
        flushblock!(w)
        w.fsync ? syncfile(w.sink) : flush(w.sink)
        if w.path !== nothing
            w.ownsink && close(w.sink)
            if w.temppath !== nothing
                chmod(w.temppath, publishmode(w.path))          # mktemp creates owner-only files
                Base.Filesystem.rename(w.temppath, w.path)
            end
        end
        w.closed = true
    catch e
        poison!(w, e)
        w.closed = true
        try
            abortcleanup(w)
        catch cleanup
            throw(CompositeException(Any[e, cleanup]))
        end
        rethrow()
    end
    return nothing
end

# ---- Avro.write / tobuffer --------------------------------------------------------------------------

"""
    Avro.write(dst, source; schema=nothing, codec=nothing, level=nothing, metadata=nothing, block_bytes=64*1024,
               name=nothing, namespace=nothing, append=false, sync=nothing, atomic=true, fsync=false,
               allow_invalid_names=false, allow_invalid_defaults=false, limits=Limits()) -> dst

Write a container file from a Tables.jl source (a `DataFrame`, a vector of `NamedTuple`s, a column table,
`Avro.Table`, `Avro.Rows`) or a collection of datums (a vector or generator of structs or plain values;
generators are collected first, so use `Avro.Writer` to stream). Schema precedence: an explicit `schema=`,
the schema an Avro source retains, the schema of the file being appended to, else a conventional derivation:
`Avro.schema(T; name, namespace)` for a collection of datums, and from the source's column types for a table
(a record named `name`, default `"Record"`), narrowing abstract column types to the types of the values they
hold. A table column the schema has no field for is an `ArgumentError`. `metadata` defaults to the user
metadata an Avro source retains, so a rewrite keeps it. `Tables.partitions` become block boundaries (each
non-empty partition ends a block). `append=true` adds blocks to the existing file at path `dst` (see
`Avro.Writer`).
"""
function write(dst::Union{AbstractString,IO}, table; schema::Union{Nothing,Schema}=nothing,
               name::Union{Nothing,AbstractString}=nothing, namespace::Union{Nothing,AbstractString}=nothing,
               metadata=nothing, append::Bool=false, limits::Limits=Limits(), kw...)
    s = something(schema, retainedschema(table), Some(nothing))
    if s === nothing && append && dst isa AbstractString && isfile(dst) && filesize(dst) > 0
        s = Reader(r -> r.schema, dst; mmap=false, limits=limits, allow_invalid_names=get(kw, :allow_invalid_names, false),
                   allow_invalid_defaults=get(kw, :allow_invalid_defaults, false))
    end
    entries = something(metadata, append ? Dict{String,Vector{UInt8}}() : retainedmetadata(table))
    newwriter(s) = Writer(dst, s; limits=limits, append=append, metadata=entries, kw...)
    if table isa Rows && getfield(table, :mode) !== :generic      # typed and non-record rows write datum-wise
        Writer(dst, s; limits=limits, append=append, metadata=entries, kw...) do w
            foreach(v -> push!(w, v), table)
        end
        return dst
    end
    partitions = table isa Rows ? RowsPartitions(table) : Tables.partitions(table)    # `Rows` re-encodes one block at a time
    if !(table isa Rows || Tables.istable(table)) && partitions isa Tuple{Any}
        isdatumcollection(table) ||
            throw(ArgumentError("`Avro.write(dst, x)` writes Tables.jl sources and vectors or iterators of datums, not $(typeof(table)); use `Avro.encode!` to write one datum"))
        datums = table isa AbstractVector && isconcretetype(eltype(table)) ? table : [x for x in table]
        Tables.istable(datums) && return write(dst, datums; schema=s, name=name, namespace=namespace, metadata=metadata, append=append, limits=limits, kw...)
        T = eltype(datums)
        s === nothing && (s = Avro.schema(T; name=name, namespace=namespace))
        Writer(dst, s; limits=limits, append=append, metadata=entries, kw...) do w
            pushrowsof!(w, i -> datums[firstindex(datums) + i - 1], T, length(datums))
        end
        return dst
    end
    w = nothing
    ok = false
    try
        for part in partitions
            rows = Tables.rows(part)
            if s === nothing
                part, rows, ts = derivationschema(part, rows)
                s = Avro.schema(ts; name=something(name, "Record"), namespace=something(namespace, ""))
            end
            ts = Tables.schema(rows)
            ts === nothing || checkcolumns(ts.names, s)
            w === nothing && (w = newwriter(s))
            pushpartition!(w, part, rows)
            flushblock!(w)
        end
        if w === nothing
            s === nothing && throw(ArgumentError("an empty partitioned source reports no Tables.schema; pass `schema=`"))
            w = newwriter(s)
        end
        ok = true
    finally
        w === nothing || (ok ? close(w) : close(w; abort=true))
    end
    return dst
end

# What `Avro.write` takes as a collection of datums: vectors other than a byte string, and lazy iterators.
function isdatumcollection(x)
    x isa AbstractVector{UInt8} && return false
    return x isa Union{AbstractVector,Base.Generator,Channel} || parentmodule(typeof(x)) === Base.Iterators
end

# A table column the record has no field for would silently vanish.
function checkcolumns(@nospecialize(names), s::Schema)
    s isa RecordSchema || return nothing
    extra = Symbol[]
    for n in names
        haskey(s.fieldindex, Symbol(n)) || push!(extra, Symbol(n))
    end
    isempty(extra) || throw(ArgumentError("the source has columns $extra that record $(fullname(s)) lacks; select the columns to write first"))
    return nothing
end

# A column type that says nothing about how to derive a schema: `Any`, `Real` and the like.
function isopentype(@nospecialize(T))
    return any(m -> isabstracttype(m) && !(m <: Union{AbstractString,AbstractVector,AbstractDict}), Base.uniontypes(T))
end

# The partition, its rows and the `Tables.Schema` a record schema derives from. Rows reporting no schema
# (differing types, a plain iterator) become a row table whose column types are the union of the types its
# rows hold, and open column types likewise become the union of the types the column holds.
function derivationschema(part, rows)
    ts = Tables.schema(rows)
    (ts === nothing || any(isopentype, ts.types)) || return (part, rows, ts)
    return narrowedschema(part, rows, ts)
end

# Kept apart and unspecialized, so a source with concrete column types never compiles it.
@noinline function narrowedschema(@nospecialize(part), @nospecialize(rows), @nospecialize(ts))
    if ts === nothing || !Tables.columnaccess(typeof(part))
        table = Tables.dictrowtable(rows)
        ts = Tables.schema(table)
        isempty(ts.names) && throw(ArgumentError("the source has no rows to derive its columns from; pass `schema=`"))
        return (table, Tables.rows(table), ts)
    end
    columns = Tables.columns(part)
    types = Type[]
    for (name, T) in zip(ts.names, ts.types)
        column = Tables.getcolumn(columns, name)
        push!(types, isopentype(T) && !isempty(column) ? Union{unique(typeof.(column))...} : T)
    end
    return (part, rows, Tables.Schema(ts.names, types))
end

"""
Push one partition. A column table whose columns match the record schema's fields in order (up to the
aligned-row width) is pushed as typed `NamedTuple` rows, which take the writer's aligned fast path, as
is an aligned vector of one concrete `NamedTuple` type. Other column tables and strided NamedTuple
vectors with every field present use per-field cells; other sources are pushed row by row.
"""
function pushpartition!(w::Writer, part, rows)
    if part isa Table && isempty(Tables.columnnames(part))      # no column carries a zero-column table's row count
        foreach(_ -> push!(w, (;)), 1:length(part))
        return w
    end
    rows isa AbstractVector && isconcretetype(eltype(rows)) && eltype(rows) <: NamedTuple &&
        return pushnamedtuples!(w, rows)
    if Tables.columnaccess(typeof(part))
        cols = Tables.columns(part)
        names = Tables.columnnames(cols)
        if alignedcolumns(w.schema, names)
            symbols = Tuple(Symbol(n) for n in names)
            return pushcolumns!(w, NamedTuple{symbols}(Tuple(Tables.getcolumn(cols, i) for i in eachindex(symbols))))
        end
        pushcells!(w, cols) && return w
    end
    known = Tables.schema(rows) !== nothing
    for row in rows
        known || checkcolumns(row isa AbstractDict ? keys(row) : Tables.columnnames(row), w.schema)
        push!(w, row isa Union{NamedTuple,Record,AbstractDict,Tables.AbstractRow} ? row : TableRow(row))
    end
    return w
end

function alignedcolumns(s::Schema, names)
    s isa RecordSchema || return false
    0 < length(names) == length(s.fields) <= 32 || return false
    for (i, n) in enumerate(names)
        (n isa Union{Symbol,AbstractString} && String(n) == s.fields[i].name) || return false
    end
    return true
end

function pushcolumns!(w::Writer, cols::NamedTuple{K}) where {K}
    return pushcolumns!(w, cols, NamedTuple{K,Tuple{map(eltype, values(cols))...}})
end

function pushcolumns!(w::Writer, cols::NamedTuple, ::Type{R}) where {R<:NamedTuple}
    n = length(first(cols))
    for c in cols
        length(c) == n || throw(ArgumentError("table columns have different lengths"))
    end
    return pushrowsof!(w, i -> R(ntuple(j -> cols[j][firstindex(cols[j]) + i - 1],
                                       Val(fieldcount(R)))), R, n)
end

"""
Push every row of the column table `cols` through one cell per schema field. Returns `false`, having
pushed nothing, when a field has no vector column (the row path then applies).
"""
function pushcells!(w::Writer, cols)
    p = w.plan
    (p isa WRecord && !isempty(p.fields)) || return false
    positions = recordrowpositions(p, cols)
    any(==(0), positions) && return false
    n = length(Tables.getcolumn(cols, positions[1]))
    cells = Vector{WriteCell}(undef, length(positions))
    for (k, j) in enumerate(positions)
        column = Tables.getcolumn(cols, j)
        column isa AbstractVector || return false
        length(column) == n || throw(ArgumentError("table columns have different lengths"))
        cells[k] = ColumnCell(p.fields[k], column)
    end
    pushcellrows!(w, cells, n)
    return true
end

function pushcellrows!(w::Writer, cells::Vector{WriteCell}, n::Int)
    checkwritable(w)
    row = CellRow(0)
    try
        for i in 1:n
            row.i = i
            pushaligneddatum!(w, row, cells)
        end
    catch e
        poison!(w, e)
        rethrow()
    end
    return w
end

function pushnamedtuples!(w::Writer, rows::AbstractVector{R}) where {R<:NamedTuple}
    isempty(rows) && return w
    if !alignedcolumns(w.schema, fieldnames(R)) && pushrowcells!(w, rows)
        return w
    end
    return pushrowsof!(w, i -> @inbounds(rows[firstindex(rows) + i - 1]), R, length(rows))
end

"Use typed field views over the original rows without materializing columns or boxing whole rows."
function pushrowcells!(w::Writer, rows::AbstractVector{R}) where {R<:NamedTuple}
    # Custom vectors may compute or consume a row on access; only strided storage permits field views.
    rows isa StridedVector{R} || return false
    p = w.plan
    (p isa WRecord && !isempty(p.fields)) || return false
    positions = recordfieldpositions(p.schema, R)
    any(==(0), positions) && return false
    cells = Vector{WriteCell}(undef, length(positions))
    for (k, j) in enumerate(positions)
        cells[k] = RowCell(p.fields[k], rows, j)
    end
    pushcellrows!(w, cells, length(rows))
    return true
end

"Push `row(i)` for `i` in `1:n`; aligned rows skip the dynamic plan lookup."
function pushrowsof!(w::Writer, row::F, ::Type{R}, n::Int) where {F,R}
    plans = alignedplans(w.plan, R)
    if plans === nothing
        for i in 1:n
            push!(w, row(i))
        end
        return w
    end
    return pushalignedrows!(w, row, plans, n)
end

"Specialised on the plans so no row is boxed."
function pushalignedrows!(w::Writer, row::F, plans::P, n::Int) where {F,P<:Tuple}
    checkwritable(w)
    try
        for i in 1:n
            pushaligneddatum!(w, row(i), plans)
        end
    catch e
        poison!(w, e)
        rethrow()
    end
    return w
end

"""
    Avro.tobuffer(table; kw...) -> IOBuffer

`Avro.write` into a fresh, rewound `IOBuffer`.
"""
function tobuffer(table; kw...)
    io = IOBuffer()
    write(io, table; kw...)
    seekstart(io)
    return io
end

# ---- inspect ----------------------------------------------------------------------------------------

"""
    Avro.InspectReport

`Avro.inspect`'s diagnostic summary: the codec, permissively parsed schema, block/datum counts, and the
issues found (legacy codec names, repaired names/defaults, structural problems).
"""
struct InspectReport
    codec::Union{Nothing,String}
    schema::Union{Nothing,Schema}
    blocks::Int
    datums::Int
    compressedbytes::Int
    issues::Vector{String}
end

const INSPECT_ISSUE_CAPACITY = 256

"Append an issue; the last slot holds the omission marker once the report is full."
function inspectissue!(issues::Vector{String}, issue::String)
    n = length(issues)
    n < INSPECT_ISSUE_CAPACITY - 1 && return (push!(issues, issue); nothing)
    n == INSPECT_ISSUE_CAPACITY - 1 && push!(issues, "additional issues were omitted")
    return nothing
end

function inspectissue!(issues::Vector{String}, error::AvroError)
    return inspectissue!(issues, sprint(showerror, error))
end

function Base.show(io::IO, ::MIME"text/plain", r::InspectReport)
    println(io, "Avro.InspectReport:")
    println(io, "  codec:  ", something(r.codec, "?"))
    println(io, "  schema: ", r.schema === nothing ? "?" : kind(r.schema))
    println(io, "  blocks: ", r.blocks, " (", r.datums, " datums, ", r.compressedbytes, " compressed payload bytes)")
    if isempty(r.issues)
        print(io, "  no issues found")
    else
        print(io, "  issues:")
        for issue in r.issues
            print(io, "\n   - ", issue)
        end
    end
    return nothing
end

"""
    Avro.inspect(src; limits=Limits()) -> Avro.InspectReport

A permissive diagnostic parse of a container source: never mutates it, tolerates repaired schemas and
legacy codec names, and stops at (and reports) the first structural problem.
"""
function inspect(src; limits::Limits=Limits())
    issues = String[]
    codecname = nothing
    schemaout = nothing
    blocks = 0
    datums = 0
    cbytes = 0
    source = opensource(src)
    try
        h = readheader(source, limits; legacy=:avrojl1, allow_invalid_names=true, allow_invalid_defaults=true)
        codecname = h.codecname
        schemaout = h.schema
        h.codecname == "zstd" &&
            inspectissue!(issues, "codec \"zstd\" is Avro.jl ≤ 1.1.2's name for zstandard; read with legacy=:avrojl1")
        cname = try
            readercodec(h.codecname, :avrojl1)
        catch e
            e isa AvroError || rethrow()
            e isa UnsupportedCodecError && e.package === nothing ?
                inspectissue!(issues, string("unknown codec ", sprint(escapejson, h.codecname))) : inspectissue!(issues, e)
            nothing
        end
        gi = graphinfo(h.schema)
        gi.repaired_names &&
            inspectissue!(issues, "the schema contains invalid names; read with allow_invalid_names=true")
        gi.repaired_defaults &&
            inspectissue!(issues, "the schema contains invalid defaults; read with allow_invalid_defaults=true")
        inspectdecimalwarnings!(issues, h.schema)
        if cname !== nothing
            plan = readplan(h.schema; mode=:framing)
            while !sourceeof(source)
                count = sourcevarint(source)
                count >= 0 || throw(DataError("negative block count $count", position(source)))
                size = sourcevarint(source)
                size >= 0 || throw(DataError("negative block size $size", position(source)))
                size <= limits.max_bytes || throw(LimitError(:max_bytes, size, limits.max_bytes))
                payload = sourcepayload(source, Int(size))
                checksync(source, h.sync, blocks + 1)
                decoded = decompressblock(cname, payload, limits)
                n = Int(count)
                decoder = Decoder(decoded, limits)
                for _ in 1:n
                    decode(plan, decoder)
                end
                if decoder.pos != length(decoded) + 1
                    cname === :null ||
                        throw(DataError("block $(blocks + 1) declares $n datums but they consume $(decoder.pos - 1) of $(length(decoded)) bytes", decoder.pos))
                    inspectissue!(issues, string("block ", blocks + 1, " has ", length(decoded) - decoder.pos + 1,
                                                 " trailing payload bytes after ", n,
                                                 " datums (Avro.jl ≤ 1.1.2 null-codec padding; read with legacy=:avrojl1)"))
                end
                blocks += 1
                datums += n
                cbytes += Int(size)
            end
        end
    catch e
        e isa AvroError || rethrow()
        inspectissue!(issues, e)
    finally
        closesource(source)
    end
    return InspectReport(codecname, schemaout, blocks, datums, cbytes, issues)
end

function inspectdecimalwarnings!(issues::Vector{String}, s::Schema)
    return inspectdecimalwarnings!(issues, s, falses(graphinfo(s).nodes))
end

function inspectdecimalwarnings!(issues::Vector{String}, s::Schema, seen::BitVector)
    length(issues) == INSPECT_ISSUE_CAPACITY && return nothing
    index = Int(nodeid(s)) + 1
    seen[index] && return nothing
    seen[index] = true
    if s isa FixedSchema && s.logical isa DecimalLogical && s.size != 16
        inspectissue!(issues, string("fixed decimal \"", fullname(s), "\" has size ", s.size,
                                     ": Avro.jl ≤ 1.1.2 wrote 16 bytes regardless, so 1.x files with this schema are misframed"))
    elseif s isa ArraySchema
        inspectdecimalwarnings!(issues, s.items, seen)
    elseif s isa MapSchema
        inspectdecimalwarnings!(issues, s.values, seen)
    elseif s isa UnionSchema
        for branch in s.branches
            inspectdecimalwarnings!(issues, branch, seen)
        end
    elseif s isa RecordSchema
        for field in s.fields
            inspectdecimalwarnings!(issues, field.schema, seen)
        end
    end
    return nothing
end
