# Object container files (plan §4.9): a strict block reader over memory-mapped, byte and streamed
# sources; a writer with the atomic path contract and the full failure contract. Every limit a reader
# enforces the writer enforces; `max_codec_memory` is checked per member read and per frame written.

import Mmap

const MAGIC = (UInt8('O'), UInt8('b'), UInt8('j'), 0x01)

# ---- sources ----------------------------------------------------------------------------------------

mutable struct BytesSource{B<:AbstractVector{UInt8}}
    const buf::B
    pos::Int
    const stop::Int
end

function BytesSource(buf::AbstractVector{UInt8}, pos::Int)
    return BytesSource(buf, pos, length(buf))
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

"Exactly `n` payload bytes: a view for byte sources (caller-owned), an owned charged buffer for streams."
function sourcepayload(s::BytesSource, n::Int, budget::Budget)
    n <= s.stop - s.pos + 1 || throw(DataError("truncated file", s.pos))
    out = view(s.buf, s.pos:s.pos + n - 1)
    s.pos += n
    return out
end

function sourcepayload(s::StreamSource, n::Int, budget::Budget)
    reserve!(budget, bytesbytes(n))
    out = Vector{UInt8}(undef, n)
    allocated!(budget, bytesbytes(n))
    consumed = readbytes!(s.io, out, n)
    s.pos += consumed
    consumed == n || throw(DataError("truncated file", s.pos))
    return out
end

function payloadcharge(::BytesSource, n::Int)
    return 0
end

function payloadcharge(::StreamSource, n::Int)
    return bytesbytes(n)
end

function closesource(::BytesSource)
    return nothing
end

function closesource(s::StreamSource)
    return s.owned ? close(s.io) : nothing
end

function opensource(src::Vector{UInt8}; mmap::Bool=true)
    return BytesSource(src, 1)
end

function opensource(src::IOBuffer; mmap::Bool=true)
    return BytesSource(src.data, 1, src.size)
end

function opensource(src::IO; mmap::Bool=true)
    return StreamSource(src, false)
end

function opensource(src::AbstractString; mmap::Bool=true)
    mmap || return StreamSource(open(src, "r"), true)
    return BytesSource(open(io -> Mmap.mmap(io, Vector{UInt8}), src, "r"), 1)
end

function opensource(src::IOBuffer, ::Val{:trim})
    return BytesSource(src.data, 1, src.size)
end

# ---- header -----------------------------------------------------------------------------------------

"The exact retained storage of the parallel metadata key and value indexes."
function metadataindexbytes(n::Int)
    return checked_add(vectorbytes(String, n), vectorbytes(Vector{UInt8}, n))
end

"Allocate the two exact-capacity metadata indexes after reserving each one."
function metadataindexes(capacity::Int, budget::Budget)
    keycharge = vectorbytes(String, capacity)
    reserve!(budget, keycharge)
    newkeys = try
        keys = Vector{String}(undef, capacity)
        allocated!(budget, keycharge)
        keys
    catch
        unreserve!(budget, keycharge)
        rethrow()
    end
    valcharge = vectorbytes(Vector{UInt8}, capacity)
    reserve!(budget, valcharge)
    newvals = try
        vals = Vector{Vector{UInt8}}(undef, capacity)
        allocated!(budget, valcharge)
        vals
    catch
        newkeys = nothing
        release!(budget, keycharge)                    # the key vector allocated above dies with this frame
        unreserve!(budget, valcharge)
        rethrow()
    end
    return (newkeys, newvals)
end

"One owned, charged copy of a metadata payload (a stream buffer is already owned and charged)."
function ownedmetadatabytes(payload, len::Int, budget::Budget)
    payload isa Vector{UInt8} && return payload
    charge = bytesbytes(len)
    reserve!(budget, charge)                           # reserve the byte-source copy before allocation
    try
        buffer = Vector{UInt8}(payload)
        allocated!(budget, charge)
        return buffer
    catch
        unreserve!(budget, charge)
        rethrow()
    end
end

"Read one metadata entry into an owned key string and value buffer, enforcing the running byte total."
function readmetadataentry(s::BlockSource, limits::Limits, budget::Budget, total::Int)
    klen = sourcevarint(s)
    klen >= 0 || throw(DataError("negative metadata key length $klen", position(s)))
    klen <= limits.max_metadata_bytes ||
        throw(LimitError(:max_metadata_bytes, Int(klen), limits.max_metadata_bytes,
                         :max_metadata_bytes, :decode))
    kbytes = sourcepayload(s, Int(klen), budget)
    validutf8(kbytes, 1, length(kbytes)) || throw(DataError("metadata key is not valid UTF-8", position(s)))
    keybuffer = ownedmetadatabytes(kbytes, Int(klen), budget)
    key = takeownedstring!(keybuffer, budget)
    keybuffer = nothing
    vlen = sourcevarint(s)
    vlen >= 0 || throw(DataError("negative metadata value length $vlen", position(s)))
    vlen <= limits.max_metadata_bytes ||
        throw(LimitError(:max_metadata_bytes, Int(vlen), limits.max_metadata_bytes,
                         :max_metadata_bytes, :decode))
    total = checked_add(total, Int(klen) + Int(vlen))
    total <= limits.max_metadata_bytes || throw(LimitError(:max_metadata_bytes, total, limits.max_metadata_bytes, :max_metadata_bytes, :decode))
    value = ownedmetadatabytes(sourcepayload(s, Int(vlen), budget), Int(vlen), budget)
    return (key, value, total)
end

"Replace the metadata index vectors at exact `newcap` (old storage released after the copy, §4.4)."
function resizemetadata(keys::Vector{String}, vals::Vector{Vector{UInt8}}, oldcap::Int, newcap::Int,
                        n::Int, budget::Budget)
    newkeys, newvals = metadataindexes(newcap, budget)
    copyto!(newkeys, 1, keys, 1, n)
    copyto!(newvals, 1, vals, 1, n)
    release!(budget, metadataindexbytes(oldcap))
    return (newkeys, newvals)
end

"Every metadata entry as exact-length key/value index vectors (duplicates settle in `buildmap`)."
function readmetadatapairs(s::BlockSource, limits::Limits, budget::Budget)
    keycap = min(8, limits.max_metadata_entries)
    keys, vals = metadataindexes(keycap, budget)
    nkeys = 0
    total = 0
    values0 = budget.values
    input0 = budget.input_bytes
    beginworkdefer!(budget)
    complete = false
    try
        while true
            count = sourcevarint(s)
            count == 0 && break
            size = -1
            if count < 0
                count == typemin(Int64) && throw(DataError("metadata block count typemin(Int64)", position(s)))
                count = -count
                size = sourcevarint(s)
                size >= 0 || throw(DataError("negative metadata block size $size", position(s)))
                size <= limits.max_metadata_bytes ||
                    throw(LimitError(:max_metadata_bytes, Int(size), limits.max_metadata_bytes,
                                     :max_metadata_bytes, :decode))
            end
            blockstart = position(s)
            count <= limits.max_metadata_entries || throw(LimitError(:max_metadata_entries, Int(count), limits.max_metadata_entries, :max_metadata_entries, :decode))
            for _ in 1:count
                countvalues!(budget)
                nkeys < limits.max_metadata_entries || throw(LimitError(:max_metadata_entries, nkeys + 1, limits.max_metadata_entries, :max_metadata_entries, :decode))
                key, value, total = readmetadataentry(s, limits, budget, total)
                if nkeys == keycap
                    newcap = min(max(1, checked_mul(2, keycap)), limits.max_metadata_entries)
                    keys, vals = resizemetadata(keys, vals, keycap, newcap, nkeys, budget)
                    keycap = newcap
                end
                nkeys += 1
                keys[nkeys] = key
                vals[nkeys] = value
            end
            if size >= 0
                consumed = position(s) - blockstart
                consumed == size || throw(DataError("metadata block declares $size bytes but its entries consume $consumed", position(s)))
            end
        end
        addinput!(budget, total)
        complete = true
    finally
        endworkdefer!(budget)
    end
    if complete
        checkworkscope!(budget, budget.values - values0,
                        budget.input_bytes - input0)
        checkoperationwork!(budget)
    end
    keycap == nkeys || ((keys, vals) = resizemetadata(keys, vals, keycap, nkeys, nkeys, budget))
    return (keys, vals)
end

"The owned `avro.codec` name of a parsed header (\"null\" when the entry is absent)."
function headercodecname(metadata, budget::Budget, s::BlockSource)
    codecbytes = get(metadata, "avro.codec", nothing)
    codecbytes === nothing && return ownedstringcopy("null", budget)
    validutf8(codecbytes, 1, length(codecbytes)) || throw(DataError("avro.codec is not valid UTF-8", position(s)))
    codecvectorsize = bytesbytes(length(codecbytes))
    reserve!(budget, codecvectorsize)
    codeccopy = try
        bytes = copy(codecbytes)
        allocated!(budget, codecvectorsize)
        bytes
    catch
        unreserve!(budget, codecvectorsize)
        rethrow()
    end
    codecshell = stringbytes(0)
    reserve!(budget, codecshell)
    codecname = String(codeccopy)
    allocated!(budget, codecshell)
    codeccopy = nothing
    release!(budget, bytesbytes(0))
    return codecname
end

"The parsed container header: metadata (duplicates rejected), schema, codec name and sync marker."
function readheader(s::BlockSource, limits::Limits, budget::Budget; legacy, allow_invalid_names::Bool, allow_invalid_defaults::Bool)
    for m in MAGIC
        (sourceeof(s) || sourcebyte(s) != m) && throw(DataError("not an Avro object container file (bad magic)", position(s)))
    end
    keys, vals = readmetadatapairs(s, limits, budget)
    metadata = buildmap(Vector{UInt8}, keys, vals, budget; duplicateposition=position(s))
    sync = ntuple(_ -> sourcebyte(s), 16)
    schemabytes = get(metadata, "avro.schema", nothing)
    schemabytes === nothing && throw(DataError("the container has no avro.schema", position(s)))
    schema = parseschemaimpl(schemabytes, legacy === :avrojl1; allow_invalid_names=allow_invalid_names,
                             allow_invalid_defaults=allow_invalid_defaults, limits=limits, budget=budget)
    codecname = headercodecname(metadata, budget, s)
    return (metadata=metadata, schema=schema, codecname=codecname, sync=sync)
end

# ---- Reader -----------------------------------------------------------------------------------------

"""
    Avro.Reader(src; limits=Limits(), legacy=nothing, decimal_byteorder=:big, allow_invalid_names=false,
                allow_invalid_defaults=false, validate=:strict, mmap=true)

A block-level container reader over a file path (memory-mapped by default; `mmap=false` streams), a byte
vector (caller-owned), an `IOBuffer` (its written bytes) or a streaming `IO`. `eachblock(r)` yields
`(count, bytes)` with the owned decompressed block; `eachdatum(r)` yields the schema's generic values;
`close(r)` is idempotent (the do-block form closes on exit; a finalizer closes a path-owned source as a
fallback). `legacy=:avrojl1` tolerates the two unambiguous Avro.jl ≤ 1.1.2 defects; `decimal_byteorder=:little`
explicitly reinterprets 1.x native-endian decimals.
"""
mutable struct Reader
    source::Union{Nothing,BlockSource}
    const schema::Schema
    const plan::ReadPlan
    const span::SpanPlan
    const codecname::Symbol
    const codec::Any
    const metadata::Map{Vector{UInt8}}
    const sync::NTuple{16,UInt8}
    const limits::Limits
    const budget::Budget
    const validate::Symbol
    const legacy::Union{Nothing,Symbol}
    blockindex::Int
    closed::Bool
    warned::Bool
end

"Close a reader source and its budget. Explicit close reports the source failure after guard cleanup."
function closereaderresources(source, budget::Budget; suppress::Bool=false)
    failure = nothing
    try
        source === nothing || closesource(source)
    catch err
        failure = err
    end
    try
        close!(budget)
    catch err
        failure === nothing && (failure = err)
    end
    suppress || failure === nothing || throw(failure)
    return nothing
end

function Reader(src; limits::Limits=Limits(), legacy::Union{Nothing,Symbol}=nothing, decimal_byteorder::Symbol=:big,
                allow_invalid_names::Bool=false, allow_invalid_defaults::Bool=false, validate::Symbol=:strict, mmap::Bool=true)
    validate in (:strict, :fast) || throw(ArgumentError("validate must be :strict or :fast"))
    decimal_byteorder in (:big, :little) || throw(ArgumentError("decimal_byteorder must be :big or :little"))
    legacy in (nothing, :avrojl1) || throw(ArgumentError("legacy must be nothing or :avrojl1"))
    budget = Budget(limits)
    source = nothing
    r = try
        source = src isa IOBuffer ? opensource(src, Val(:trim)) : opensource(src; mmap=mmap)
        h = readheader(source, limits, budget; legacy=legacy, allow_invalid_names=allow_invalid_names, allow_invalid_defaults=allow_invalid_defaults)
        metadata = h.metadata
        parsed = h.schema
        syncmarker = h.sync
        codecname = h.codecname
        h = nothing
        cname, codec = readercodec(codecname, limits, legacy)
        codeccharge = stringbytes(sizeof(codecname))
        codecname = nothing
        release!(budget, codeccharge)
        plan = readplan(parsed; budget=budget, little=decimal_byteorder === :little)
        spans = spanplan(parsed; budget=budget)
        Reader(source, parsed, plan, spans, cname, codec, metadata, syncmarker, limits, budget,
               validate, legacy, 0, false, false)
    catch
        try
            closereaderresources(source, budget; suppress=true)
        finally
            rethrow()
        end
    end
    finalizer(r) do x
        if !x.closed
            source = x.source
            x.source = nothing
            x.closed = true
            closereaderresources(source, x.budget; suppress=true)
        end
        return nothing
    end
    return r
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
    return closereaderresources(source, r.budget)
end

function checkopen(r::Reader)
    return r.closed ? throw(ArgumentError("the reader is closed")) : nothing
end

"""
    Avro.metadata(r) -> Avro.Map{Vector{UInt8}}; Avro.codec(r) -> Symbol; Avro.sync(r) -> NTuple{16,UInt8}
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

struct BlockWork
    values0::Int
    input0::Int
end

function blockstaticvalues(budget::Budget, count::Int, staticvalues::Int)
    staticvalues < 0 && return count
    count == 0 && return 0
    maximum = budget.limits.max_total_values
    staticvalues <= div(maximum, count) ||
        throw(limiterror(budget, :max_total_values, typemax(Int), maximum))
    return count * staticvalues
end

function checkblocklower!(budget::Budget, work::BlockWork, count::Int,
                          staticvalues::Int=-1)
    members = budget.values - work.values0
    input = budget.input_bytes - work.input0
    datumvalues = blockstaticvalues(budget, count, staticvalues)
    checkworkscope!(budget, checkedvalueadd(budget, members, datumvalues), input)
    checkprojectedvalues!(budget, datumvalues)
    return nothing
end

function setblockscope!(decoder::Decoder, work::BlockWork)
    decoder.blockvalues0 = work.values0
    decoder.blockinput0 = work.input0
    return decoder
end

function finishblockwork!(budget::Budget, work::BlockWork)
    values = budget.values - work.values0
    input = budget.input_bytes - work.input0
    checkworkscope!(budget, values, input)
    checkoperationwork!(budget)
    return nothing
end

function exactlegacyblock(bytes::Vector{UInt8}, count::Int, reader::Reader)
    position = 1
    values = 0
    decoder = Decoder(bytes, reader.budget; validate=reader.validate)
    for _ in 1:count
        datumspan!(reader.span, decoder, position,
                   satadd(reader.budget.values, values),
                   reader.budget.input_bytes)
        span = decoderspan(decoder)
        position = span.next
        values = satadd(values, span.values)
    end
    position == length(bytes) + 1 && return bytes
    reader.codecname === :null ||
        throw(DataError("block $(reader.blockindex) declares $count datums but they consume $(position - 1) of $(length(bytes)) bytes", position))
    reader.warned ||
        (@warn "accepting trailing bytes after $count datums in a null-codec block (legacy=:avrojl1; Avro.jl ≤ 1.1.2 sizing cushion)" source = 1; reader.warned = true)
    newlength = position - 1
    charge = bytesbytes(newlength)
    reserve!(reader.budget, charge)
    exact = Vector{UInt8}(undef, newlength)
    allocated!(reader.budget, charge)
    copyto!(exact, 1, bytes, 1, newlength)
    release!(reader.budget, bytesbytes(length(bytes)))
    removeinput!(reader.budget, length(bytes) - newlength)
    return exact
end

"Read the next block into owned decompressed bytes; `nothing` at a clean end of the file."
function nextblock!(r::Reader; walk::Bool=true)
    checkopen(r)
    source = r.source::BlockSource
    sourceeof(source) && return nothing
    checkpoint = budgetcheckpoint(r.budget)
    work = BlockWork(r.budget.values, r.budget.input_bytes)
    try
        count = sourcevarint(source)
        (0 <= count <= r.limits.max_block_count) || (count < 0 ? throw(DataError("negative block count $count", position(source))) :
                                                     throw(LimitError(:max_block_count, Int(count), r.limits.max_block_count, :max_block_count, :decode)))
        size = sourcevarint(source)
        (0 <= size <= r.limits.max_block_bytes) || (size < 0 ? throw(DataError("negative block size $size", position(source))) :
                                                    throw(LimitError(:max_block_bytes, Int(size), r.limits.max_block_bytes, :max_block_bytes, :decode)))
        r.blockindex += 1
        r.blockindex <= r.limits.max_blocks || throw(LimitError(:max_blocks, r.blockindex, r.limits.max_blocks, :max_blocks, :decode))
        addblocks!(r.budget)
        addrows!(r.budget, Int(count))
        payload = sourcepayload(source, Int(size), r.budget)
        for i in 1:16
            (sourceeof(source) ? throw(DataError("truncated file", position(source))) : sourcebyte(source)) == r.sync[i] ||
                throw(DataError("sync marker mismatch after block $(r.blockindex)", position(source)))
        end
        addinput!(r.budget, varintlength(count) + varintlength(size) + 16)
        bytes = if r.codecname === :null && payload isa Vector{UInt8}
            addinput!(r.budget, length(payload))
            addmembers!(r.budget)
            payload                                       # a streamed null-codec payload is already owned
        else
            out = decompressblock(r.codecname, r.codec, payload, r.limits, r.budget)
            release!(r.budget, payloadcharge(source, Int(size)))
            out
        end
        n = Int(count)
        r.validate === :strict && r.legacy === :avrojl1 &&
            (bytes = exactlegacyblock(bytes, n, r))
        checkblocklower!(r.budget, work, n, spanvalues(r.span))
        if walk
            d = setblockscope!(Decoder(bytes, r.budget; validate=r.validate), work)
            for _ in 1:n
                datumwork = begindatum!(d, r.span, r.plan, r.limits; active=false)
                try
                    if r.validate === :strict
                        skip(r.plan, d)
                    else
                        countvalues!(r.budget, datumwork.span.values)
                        d.pos = datumwork.span.next
                    end
                catch
                    abortdatum!(d, datumwork)
                    rethrow()
                end
                finishdatum!(d, datumwork)
            end
            d.pos == length(bytes) + 1 ||
                throw(DataError("block $(r.blockindex) declares $n datums but they consume $(d.pos - 1) of $(length(bytes)) bytes", d.pos))
            finishblockwork!(r.budget, work)
        end
        release!(r.budget, bytesbytes(length(bytes)))     # ownership transfers to the caller at yield
        return (n, bytes, work)
    catch
        rollbackreservations!(r.budget, checkpoint)
        rethrow()
    end
end

struct EachBlock
    reader::Reader
end

"""
    Avro.eachblock(r::Reader)

Iterate `(count, bytes)` pairs, `bytes` being the owned decompressed block (valid after iteration
advances). In `:strict` mode the block's datums are walked and exhaustion checked before it is yielded.
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
    return ((block[1], block[2]), nothing)
end

mutable struct EachDatum
    const reader::Reader
    bytes::Vector{UInt8}
    decoder::Union{Nothing,Decoder{Vector{UInt8}}}
    remaining::Int
    lastcharge::Int
    blockout::Int        # the block'''s cumulative decoded output (max_block_output_bytes, charged incrementally)
    blockwork::Union{Nothing,BlockWork}
end

"""
    Avro.eachdatum(r::Reader)

Iterate the file's datums as the schema's generic values (any root schema). Each yielded value is the
caller's at yield; its charge is released at the next iteration step.
"""
function eachdatum(r::Reader)
    return EachDatum(r, UInt8[], nothing, 0, 0, 0, nothing)
end

function releaseblock!(it::EachDatum, budget::Budget)
    charge = bytesbytes(length(it.bytes))
    decoder = it.decoder
    decoder === nothing || (decoder.buf = EMPTY_BYTES)
    it.decoder = nothing
    it.bytes = EMPTY_BYTES
    release!(budget, charge)
    return nothing
end

function Base.IteratorSize(::Type{EachDatum})
    return Base.SizeUnknown()
end

function Base.iterate(it::EachDatum, ::Nothing=nothing)
    checkopen(it.reader)
    b = it.reader.budget
    release!(b, it.lastcharge)
    it.lastcharge = 0
    while it.remaining == 0
        blk = nextblock!(it.reader; walk=false)
        blk === nothing && return nothing
        it.remaining = blk[1]
        it.bytes = blk[2]
        it.blockwork = blk[3]
        it.blockout = 0
        reserve!(b, bytesbytes(length(it.bytes)))     # the block is resident while its datums decode
        allocated!(b, bytesbytes(length(it.bytes)))   # decompressed by nextblock!; already resident
        it.decoder = setblockscope!(Decoder(it.bytes, b; validate=it.reader.validate),
                                    it.blockwork::BlockWork)
        if it.remaining == 0
            validateemptyblock(it.bytes, it.reader.validate)
            finishblockwork!(b, it.blockwork::BlockWork)
            it.blockwork = nothing
            releaseblock!(it, b)
        end
    end
    it.remaining -= 1
    before = b.reserved
    decoder = it.decoder::Decoder{Vector{UInt8}}
    datumwork = begindatum!(decoder, it.reader.span, it.reader.plan,
                            it.reader.limits)
    v = try
        decode(it.reader.plan, decoder)
    catch
        abortdatum!(decoder, datumwork)
        rethrow()
    end
    finishdatum!(decoder, datumwork)
    it.lastcharge = max(b.reserved - before, 0)
    it.blockout = checked_add(it.blockout, it.lastcharge + representationslot(juliatype(it.reader.schema)))
    it.blockout <= b.limits.max_block_output_bytes ||
        throw(LimitError(:max_block_output_bytes, it.blockout, b.limits.max_block_output_bytes, :max_block_output_bytes, :decode))
    if it.remaining == 0
        d = it.decoder::Decoder{Vector{UInt8}}
        if d.pos != length(it.bytes) + 1 && it.reader.validate === :strict
            throw(DataError("block datums did not consume the block exactly", d.pos))
        end
        finishblockwork!(b, it.blockwork::BlockWork)
        it.blockwork = nothing
        releaseblock!(it, b)
    end
    return (v, nothing)
end

function validateemptyblock(bytes::Vector{UInt8}, validate::Symbol)
    validate === :strict && !isempty(bytes) &&
        throw(DataError("a zero-count block has a non-empty payload", 1))
    return nothing
end

# ---- the reader-side output estimate of encoded values (plan §4.9, one consumer-independent formula) --

struct OutputEstimate
    retained::Int
    peakextra::Int
    values::Int
end

function outputtype(::WNull, x)
    return Missing
end

function outputtype(::WBool, x)
    return Bool
end

function outputtype(::WInt, x)
    return Int32
end

function outputtype(::WLong, x)
    return Int64
end

function outputtype(::WFloat, x)
    return Float32
end

function outputtype(::WDouble, x)
    return Float64
end

function outputtype(::WDate, x)
    return Date
end

function outputtype(::Union{WTimeMillis,WTimeMicros}, x)
    return Time
end

function outputtype(::WTimestamp{P}, x) where {P}
    return Timestamp{P}
end

function outputtype(::WLocalTimestamp{P}, x) where {P}
    return LocalTimestamp{P}
end

function outputtype(::WString, x)
    return String
end

function outputtype(::WBytes, x)
    return Vector{UInt8}
end

function outputtype(::WFixed, x)
    return Fixed
end

function outputtype(::WEnum, x)
    return EnumValue
end

function outputtype(::WUUIDString, x)
    return UUID
end

function outputtype(::WUUIDFixed, x)
    return UUID
end

function outputtype(::WDuration, x)
    return Duration
end

function outputtype(p::WDecimal, x)
    return p.precision <= 38 ? Decimal : WideDecimal
end

function outputtype(p::WArray, x)
    return Vector{p.eltype}
end

function outputtype(p::WMap, x)
    return Map{p.eltype}
end

function outputtype(::WRecord, x)
    return Record
end

function outputtype(p::WUnion, x)
    p.nullable == 0 && return UnionValue
    i = x isa UnionValue ? x.index : ((x === missing || x === nothing) ? p.nullable : 3 - p.nullable)
    return outputtype(p.branches[i], x isa UnionValue ? x.value : x)
end

function estimatedbox(p::WritePlan, x)
    T = outputtype(p, x)
    T === Missing && return 0
    return isbitstype(T) ? boxbytes(T) : 0
end

function estimatedelement(T::Type, estimate::OutputEstimate, p::WritePlan, x)
    (isbitstype(T) || Base.isbitsunion(T)) && return 0
    if T === Any || T isa Union
        return checked_add(estimate.retained, estimatedbox(p, x))
    end
    return estimate.retained
end

function growbufextra(T::Type, count::Int, hint::Int; shell::Bool=true)
    final = vectorbytes(T, count)
    cap = growinitialcapacity(hint)
    current = vectorbytes(T, cap)
    peak = current
    while cap < count
        newcap = grownextcapacity(cap)
        replacement = vectorbytes(T, newcap)
        peak = max(peak, checked_add(current, replacement))
        cap = newcap
        current = replacement
    end
    if cap != count
        peak = max(peak, checked_add(current, final))
    end
    extra = max(peak - final, 0)
    shell && (extra = checked_add(extra, shellbytes(GrowBuf{T})))
    return extra
end

"The final generic value storage and its maximum transient construction excess."
function estimatewithpath(f::F, diagnostics::Union{Nothing,Encoder},
                          schema::Schema, kind::UInt8,
                          name::Union{Nothing,AbstractString,Symbol}=nothing,
                          index::Int=0) where {F}
    diagnostics === nothing && return f()
    return withencodepath!(f, diagnostics, schema, kind, name, index)
end

function estimatewithschema(f::F, diagnostics::Union{Nothing,Encoder},
                            schema::Schema) where {F}
    diagnostics === nothing && return f()
    return withencodeschema!(f, diagnostics, schema)
end

function estimatevalue(p::WritePlan, x, budget::Budget,
                       diagnostics::Union{Nothing,Encoder}=nothing)::OutputEstimate
    p isa Union{WNull,WBool,WInt,WLong,WFloat,WDouble,WDate,WTimeMillis,WTimeMicros,WTimestamp,WLocalTimestamp,
                WUUIDString} && return OutputEstimate(0, 0, 1)
    if p isa WUUIDFixed
        checkvaluebytes(budget, 16)
        return OutputEstimate(0, 0, 1)
    end
    if p isa WDuration
        checkvaluebytes(budget, 12)
        return OutputEstimate(0, 0, 1)
    end
    p isa WString && return OutputEstimate(stringbytes(valuesizeof(x)), 0, 1)
    p isa WBytes && return OutputEstimate(bytesbytes(valuelength(x)), 0, 1)
    if p isa WFixed
        checkvaluebytes(budget, p.schema.size)
        return OutputEstimate(fixedbytes(p.schema.size), 0, 1)
    end
    p isa WEnum && return OutputEstimate(enumvaluebytes(), 0, 1)
    if p isa WDecimal
        x isa DataDecimals.AbstractDecimal && !(x isa Decimal) && (x = _decimalinput(x))
        x isa Union{Decimal,WideDecimal} || return OutputEstimate(0, 0, 1)
        n = p.fixedsize == 0 ? twoscomplementlength(x.unscaled) : p.fixedsize
        p.fixedsize == 0 || checkvaluebytes(budget, p.fixedsize)
        wide = p.precision > 38
        bigint = wide || n > 16
        bigbytes = bigint ? widedecimalbytes(n) : 0
        negativebytes = bigint && x.unscaled < 0 ? bytesbytes(n) : 0
        retained = wide ? bigbytes : 0
        peakextra = wide ? negativebytes : checked_add(bigbytes, negativebytes)
        return OutputEstimate(retained, peakextra, 1)
    end
    if p isa WUnion
        inner = x isa UnionValue ? x.value : x
        i = selectbranch(p, x, budget)
        estimate = estimatewithschema(diagnostics, p.schema.branches[i]) do
            estimatevalue(p.branches[i], inner, budget, diagnostics)
        end
        values = checkedvalueadd(budget, 1, estimate.values)
        p.nullable != 0 &&
            return OutputEstimate(estimate.retained, estimate.peakextra, values)
        retained = checked_add(unionvaluebytes(), checked_add(estimate.retained, estimatedbox(p.branches[i], inner)))
        return OutputEstimate(retained, estimate.peakextra, values)
    end
    if p isa WArray
        items = arrayitems(x)
        n = length(items)
        checkestimatedcount(budget, n)
        finalvector = vectorbytes(p.eltype, n)
        n == 0 && return OutputEstimate(finalvector, 0, 1)
        capacity = growinitialcapacity(n)
        backing = vectorbytes(p.eltype, capacity)
        shell = shellbytes(GrowBuf{p.eltype})
        settled = 0
        peak = checked_add(backing, shell)
        values = 1
        count = 0
        for element in items
            count = checked_add(count, 1)
            count <= n || encodeerror("array size changed during estimation", x)
            estimate = estimatewithpath(diagnostics, p.schema.items,
                                        ENCODE_PATH_INDEX, nothing,
                                        count - 1) do
                estimatevalue(p.items, element, budget, diagnostics)
            end
            parent = checked_add(checked_add(backing, shell), settled)
            childpeak = checked_add(parent, checked_add(estimate.retained, estimate.peakextra))
            peak = max(peak, childpeak)
            if count - 1 == capacity
                newcapacity = grownextcapacity(capacity)
                replacement = vectorbytes(p.eltype, newcapacity)
                overlap = checked_add(parent, checked_add(estimate.retained, replacement))
                peak = max(peak, overlap)
                capacity = newcapacity
                backing = replacement
            end
            elementbytes = estimatedelement(p.eltype, estimate, p.items, element)
            settled = checked_add(settled, elementbytes)
            peak = max(peak, checked_add(checked_add(backing, shell), settled))
            values = checkedvalueadd(budget, values, estimate.values)
        end
        count == n || encodeerror("array size changed during estimation", x)
        retained = checked_add(finalvector, settled)
        if capacity != n
            finishpeak = checked_add(checked_add(checked_add(backing, finalvector), shell), settled)
            peak = max(peak, finishpeak)
        end
        peak = max(peak, retained)
        return OutputEstimate(retained, max(peak - retained, 0), values)
    end
    if p isa WMap
        pairs = mappairs(x)
        n = length(pairs)
        checkestimatedcount(budget, n)
        if n == 0
            retained = mapbytes(p.eltype, 0)
            return OutputEstimate(retained, vectorbytes(Int32, 0), 1)
        end
        keycapacity = growinitialcapacity(0)
        valuecapacity = growinitialcapacity(n)
        keybacking = vectorbytes(String, keycapacity)
        valuebacking = vectorbytes(p.eltype, valuecapacity)
        keyshell = shellbytes(GrowBuf{String})
        valueshell = shellbytes(GrowBuf{p.eltype})
        keypayload = 0
        valuepayload = 0
        peak = checked_add(checked_add(keybacking, valuebacking), checked_add(keyshell, valueshell))
        values = 1
        count = 0
        for (key, element) in pairs
            count = checked_add(count, 1)
            count <= n || encodeerror("map size changed during estimation", x)
            key isa Union{AbstractString,Symbol} || mapkeystring(key)
            keybytes = stringbytes(valuesizeof(key))
            parents = checked_add(checked_add(keybacking, valuebacking),
                                  checked_add(keyshell, valueshell))
            payload = checked_add(keypayload, valuepayload)
            peak = max(peak, checked_add(checked_add(parents, payload), keybytes))
            if count - 1 == keycapacity
                newcapacity = grownextcapacity(keycapacity)
                replacement = vectorbytes(String, newcapacity)
                overlap = checked_add(checked_add(parents, payload),
                                      checked_add(keybytes, replacement))
                peak = max(peak, overlap)
                keycapacity = newcapacity
                keybacking = replacement
            end
            keypayload = checked_add(keypayload, keybytes)
            parents = checked_add(checked_add(keybacking, valuebacking),
                                  checked_add(keyshell, valueshell))
            estimate = if key isa Union{AbstractString,Symbol}
                estimatewithpath(diagnostics, p.schema.values,
                                 ENCODE_PATH_KEY, key) do
                    estimatevalue(p.values, element, budget, diagnostics)
                end
            else
                estimatevalue(p.values, element, budget, diagnostics)
            end
            payload = checked_add(keypayload, valuepayload)
            childpeak = checked_add(checked_add(parents, payload),
                                    checked_add(estimate.retained, estimate.peakextra))
            peak = max(peak, childpeak)
            if count - 1 == valuecapacity
                newcapacity = grownextcapacity(valuecapacity)
                replacement = vectorbytes(p.eltype, newcapacity)
                overlap = checked_add(checked_add(parents, payload),
                                      checked_add(estimate.retained, replacement))
                peak = max(peak, overlap)
                valuecapacity = newcapacity
                valuebacking = replacement
            end
            elementbytes = estimatedelement(p.eltype, estimate, p.values, element)
            valuepayload = checked_add(valuepayload, elementbytes)
            parents = checked_add(checked_add(keybacking, valuebacking),
                                  checked_add(keyshell, valueshell))
            peak = max(peak, checked_add(parents, checked_add(keypayload, valuepayload)))
            values = checkedvalueadd(budget, values, estimate.values)
        end
        count == n || encodeerror("map size changed during estimation", x)
        finalkeys = vectorbytes(String, n)
        finalvalues = vectorbytes(p.eltype, n)
        payload = checked_add(keypayload, valuepayload)
        parents = checked_add(checked_add(keybacking, valuebacking),
                              checked_add(keyshell, valueshell))
        if keycapacity != n
            peak = max(peak, checked_add(checked_add(parents, payload), finalkeys))
            keybacking = finalkeys
        end
        keyshell = 0
        parents = checked_add(checked_add(keybacking, valuebacking), valueshell)
        if valuecapacity != n
            peak = max(peak, checked_add(checked_add(parents, payload), finalvalues))
            valuebacking = finalvalues
        end
        retained = checked_add(mapbytes(p.eltype, n), payload)
        scratch = vectorbytes(Int32, cld(n, 2))
        keep = n > 1 ? vectorbytes(Bool, n) : 0
        peak = max(peak, checked_add(retained, checked_add(scratch, keep)))
        return OutputEstimate(retained, max(peak - retained, 0), values)
    end
    if p isa WRecord
        estimate, _ = estimaterecordvalue(p, x, nothing, budget,
                                          diagnostics)
        return estimate
    end
    return OutputEstimate(0, 0, 1)
end

function estimaterootvalue(p::WritePlan, x, budget::Budget,
                           diagnostics::Union{Nothing,Encoder}=nothing)
    estimate = estimatevalue(p, x, budget, diagnostics)
    retained = checked_add(planrootslot(p), estimate.retained)
    return (retained, estimate.peakextra, estimate.values)
end

function planrootslot(::WNull)
    return 0
end

function planrootslot(::WBool)
    return sizeof(Bool)
end

function planrootslot(::Union{WInt,WFloat})
    return sizeof(Int32)
end

function planrootslot(::Union{WLong,WDouble,WDate,WTimeMillis,WTimeMicros,WTimestamp,WLocalTimestamp})
    return sizeof(Int64)
end

function planrootslot(p::WDecimal)
    return p.precision <= 38 ? sizeof(Decimal) : STORAGE[].slot
end

function planrootslot(::Union{WUUIDString,WUUIDFixed})
    return sizeof(UUID)
end

function planrootslot(::WDuration)
    return sizeof(Duration)
end

function planrootslot(p::WUnion)
    p.nullable == 0 && return STORAGE[].slot
    T = p.reprtypes[3 - p.nullable]
    return representationslot(Union{Missing,T})
end

function planrootslot(::WritePlan)
    return STORAGE[].slot
end

function checkestimatedcount(budget::Budget, count::Int)
    count >= 0 || throw(EncodeError("collection length must be non-negative", "", nothing))
    count <= budget.limits.max_block_count ||
        throw(limiterror(budget, :max_block_count, count, budget.limits.max_block_count))
    return nothing
end

function valuesizeof(x::Union{AbstractString,Symbol})
    return sizeof(x)
end

function valuesizeof(x::Char)
    return ncodeunits(x)
end

function valuesizeof(x)
    return 0
end

function valuelength(x)
    return x isa AbstractVector{UInt8} ? length(x) : 0
end

function recordfieldvalue(p::WRecord, x, i::Int, budget::Union{Nothing,Budget}=nothing)
    x isa Record && return getfield(x, :values)[i]
    x isa TableRow && return rowfield(x.row, p.schema.fields[i].name, budget)
    name = p.schema.fields[i].name
    x isa AbstractDict && return dictfield(x, name, budget)
    x isa Tables.AbstractRow && return rowfield(x, name, budget)
    j = fieldposition(p, typeof(x), i, budget)
    j == 0 && encodeerror("record value lacks field \"$name\"", x)
    return getfield(x, j)
end

function addrecordestimate(retained::Int, peakextra::Int, values::Int, payload::Int,
                           field::WritePlan, fieldvalue, column::Union{Nothing,Type},
                           budget::Budget, schemafield::Field,
                           diagnostics::Union{Nothing,Encoder})
    estimate = estimatewithpath(diagnostics, schemafield.schema,
                                ENCODE_PATH_FIELD, schemafield.name) do
        estimatevalue(field, fieldvalue, budget, diagnostics)
    end
    retained = checked_add(retained,
        checked_add(estimate.retained, estimatedbox(field, fieldvalue)))
    peakextra = max(peakextra, estimate.peakextra)
    values = checkedvalueadd(budget, values, estimate.values)
    column === nothing ||
        (payload = checked_add(payload, estimatedelement(column, estimate, field, fieldvalue)))
    return (retained, peakextra, values, payload)
end

function estimaterecordvalues(p::WRecord, values, positions,
                              columns::Union{Nothing,Vector{Type}}, budget::Budget,
                              diagnostics::Union{Nothing,Encoder}=nothing)
    retained = recordbytes(length(p.fields))
    peakextra = 0
    count = 1
    payload = 0
    for (i, field) in enumerate(p.fields)
        position = positions === nothing ? i : positions[i]
        position == 0 && continue
        fieldvalue = values[position]
        fieldvalue === DICT_MISSING && continue
        column = columns === nothing ? nothing : columns[i]
        retained, peakextra, count, payload =
            addrecordestimate(retained, peakextra, count, payload,
                              field, fieldvalue, column, budget,
                              p.schema.fields[i], diagnostics)
    end
    return (OutputEstimate(retained, peakextra, count), payload)
end

function estimaterecordvalue(p::WRecord, x::Record,
                             columns::Union{Nothing,Vector{Type}}, budget::Budget,
                             diagnostics::Union{Nothing,Encoder}=nothing)
    source = getfield(x, :schema)
    values = getfield(x, :values)
    length(values) == length(source.fields) ||
        return (OutputEstimate(recordbytes(length(p.fields)), 0, 1), 0)
    source === p.schema &&
        return estimaterecordvalues(p, values, nothing, columns, budget,
                                    diagnostics)
    positions = recordpositions(length(p.fields), budget)
    try
        for (i, field) in enumerate(p.schema.fields)
            positions[i] = budgetedget(source.fieldindex, field.name, 0, budget)
        end
        return estimaterecordvalues(p, values, positions, columns, budget,
                                    diagnostics)
    finally
        release!(budget, vectorbytes(Int, length(positions)))
    end
end

function estimaterecordvalue(p::WRecord, x::AbstractDict,
                             columns::Union{Nothing,Vector{Type}}, budget::Budget,
                             diagnostics::Union{Nothing,Encoder}=nothing)
    values, boxes = recorddictvalues(p, x, budget)
    try
        return estimaterecordvalues(p, values, nothing, columns, budget,
                                    diagnostics)
    finally
        release!(budget, checked_add(vectorbytes(Any, length(values)), boxes))
    end
end

function estimaterecordvalue(p::WRecord, x::TableRow,
                             columns::Union{Nothing,Vector{Type}}, budget::Budget,
                             diagnostics::Union{Nothing,Encoder}=nothing)
    return estimaterecordrow(p, x.row, columns, budget, diagnostics)
end

function estimaterecordvalue(p::WRecord, x::Tables.AbstractRow,
                             columns::Union{Nothing,Vector{Type}}, budget::Budget,
                             diagnostics::Union{Nothing,Encoder}=nothing)
    return estimaterecordrow(p, x, columns, budget, diagnostics)
end

function estimaterecordrow(p::WRecord, row,
                           columns::Union{Nothing,Vector{Type}}, budget::Budget,
                           diagnostics::Union{Nothing,Encoder}=nothing)
    positions = recordrowpositions(p, row, budget)
    try
        retained = recordbytes(length(p.fields))
        peakextra = 0
        count = 1
        payload = 0
        for (i, field) in enumerate(p.fields)
            positions[i] == 0 && continue
            fieldvalue = Tables.getcolumn(row, positions[i])
            column = columns === nothing ? nothing : columns[i]
            retained, peakextra, count, payload =
                addrecordestimate(retained, peakextra, count, payload,
                                  field, fieldvalue, column, budget,
                                  p.schema.fields[i], diagnostics)
        end
        return (OutputEstimate(retained, peakextra, count), payload)
    finally
        release!(budget, vectorbytes(Int, length(positions)))
    end
end

function estimaterecordvalue(p::WRecord, x::T,
                             columns::Union{Nothing,Vector{Type}}, budget::Budget,
                             diagnostics::Union{Nothing,Encoder}=nothing) where {T}
    valid = x isa NamedTuple ||
        (isstructtype(T) && !(x isa AbstractArray) && !(x isa AbstractString))
    valid || return (OutputEstimate(recordbytes(length(p.fields)), 0, 1), 0)
    positions = recordfieldpositions(p, T, budget)
    try
        retained = recordbytes(length(p.fields))
        peakextra = 0
        count = 1
        payload = 0
        for (i, field) in enumerate(p.fields)
            positions[i] == 0 && continue
            fieldvalue = getfield(x, positions[i])
            column = columns === nothing ? nothing : columns[i]
            retained, peakextra, count, payload =
                addrecordestimate(retained, peakextra, count, payload,
                                  field, fieldvalue, column, budget,
                                  p.schema.fields[i], diagnostics)
        end
        return (OutputEstimate(retained, peakextra, count), payload)
    finally
        release!(budget, vectorbytes(Int, length(positions)))
    end
end

# ---- the writer's reader preflight (plan §4.4) -------------------------------------------------------

"The reader-retained charge of the block table: one entry per block (offset, size, count, cumulative rows)."
function blocktablecharge(nblocks::Int)
    return checked_add(STORAGE[].vector, checked_mul(40, nblocks))   # a 5-Int BlockEntry (R06)
end

"""
One block's transient reader-side peak (plan §4.4): the compressed and decompressed buffers, the codec's
decoder requirement, and the block's decoded output. One function for the writer preflight, sequential
reading, and the Phase 4c parallel admission arithmetic.
"""
function readerblockpeak(compressedbytes::Int, decompressedbytes::Int, outputbytes::Int, codecmemory::Int, samebuffer::Bool)
    peak = checked_add(checked_add(bytesbytes(decompressedbytes), outputbytes), codecmemory)
    samebuffer || (peak = checked_add(peak, bytesbytes(compressedbytes)))
    return peak
end

"""
The per-field slot allowance of the column consumer: the part of a field's §4.9 output estimate that an
`Avro.Table` chunk's reference slots already cover. The remainder of the estimate is the referenced
payload the chunk holds beyond its slots (0 for inline cells).
"""
function cellslack(p::WritePlan)
    p isa Union{WBool,WInt,WDate,WTimeMillis,WFloat} && return 4
    p isa Union{WLong,WTimeMicros,WTimestamp,WLocalTimestamp,WDouble} && return 8
    p isa WDuration && return 12
    p isa WUUIDFixed && return 16
    p isa WUUIDString && return STORAGE[].slot + 16
    p isa WEnum && return enumvaluebytes()
    p isa Union{WString,WBytes,WFixed} && return STORAGE[].slot
    return 0
end

"The streamed `Avro.Table` consumer projection of a record-root writer (plan §4.4)."
mutable struct TablePreflight
    const cols::Vector{Type}
    const slack::Vector{Int}
    const builderstate::Int
    chunkbytes::Int      # chunk shells and capacities of the flushed blocks
    payload::Int         # referenced payload of the flushed blocks, counted once
    rows::Int
    nblocks::Int
    pendingpayload::Int  # referenced payload of the pending block
end

function TablePreflight(readerschema::RecordSchema, p::WRecord,
                        readerplan::RecordPlan, budget::Budget)
    n = length(readerschema.fields)
    charge = checked_add(checked_add(vectorbytes(Type, n), vectorbytes(Int, n)),
                         shellbytes(TablePreflight))
    reserve!(budget, charge)
    cols = Vector{Type}(undef, n)
    slack = Vector{Int}(undef, n)
    for (index, field) in enumerate(readerschema.fields)
        cols[index] = juliatype(field.schema)
        slack[index] = cellslack(p.fields[index])
    end
    preflight = TablePreflight(cols, slack, columnbuildersstate(readerplan),
                               0, 0, 0, 0, 0)
    allocated!(budget, charge)
    return preflight
end

"The final outer state retained by a materialised Table, excluding its column payload."
function tableouterstate(ncols::Int, nblocks::Int)
    wrappers = checked_add(shellbytes(Table),
                           shellbytes(Tables.Schema{nothing,nothing}))
    return checked_add(wrappers,
        checked_add(vectorbytes(AbstractVector, ncols),
        checked_add(vectorbytes(Symbol, ncols),
        checked_add(vectorbytes(Symbol, ncols),
        checked_add(vectorbytes(Type, ncols),
                    vectorbytes(UnitRange{Int}, nblocks))))))
end

"The capacity of a zero-hint `BuildBuf` after `n` pushes."
function tablebuildcapacity(n::Int)
    capacity = 0
    while capacity < n
        capacity = max(checked_mul(2, capacity), 4)
    end
    return capacity
end

"The two streamed-Table index builders after `n` complete block pushes."
function tablebuildstate(n::Int)
    capacity = tablebuildcapacity(n)
    shells = checked_add(shellbytes(BuildBuf{Int}),
                         shellbytes(BuildBuf{Vector{AbstractVector}}))
    return checked_add(shells,
        checked_add(vectorbytes(Int, capacity),
                    vectorbytes(Vector{AbstractVector}, capacity)))
end

"The exact index-builder peak while the `n`th block is pushed into both builders."
function tablebuildpushpeak(n::Int)
    oldcapacity = tablebuildcapacity(n - 1)
    newcapacity = tablebuildcapacity(n)
    oldcapacity == newcapacity && return tablebuildstate(n)
    shells = checked_add(shellbytes(BuildBuf{Int}),
                         shellbytes(BuildBuf{Vector{AbstractVector}}))
    chunkgrowth = checked_add(vectorbytes(Vector{AbstractVector}, oldcapacity),
        checked_add(vectorbytes(Vector{AbstractVector}, newcapacity),
                    vectorbytes(Int, oldcapacity)))
    countgrowth = checked_add(vectorbytes(Vector{AbstractVector}, newcapacity),
        checked_add(vectorbytes(Int, oldcapacity), vectorbytes(Int, newcapacity)))
    return checked_add(shells, max(chunkgrowth, countgrowth))
end

"The exact index-builder peak while both builders shrink to `n` entries."
function tablebuildfinishpeak(n::Int)
    capacity = tablebuildcapacity(n)
    capacity == n && return tablebuildstate(n)
    chunkshell = shellbytes(BuildBuf{Vector{AbstractVector}})
    countshell = shellbytes(BuildBuf{Int})
    chunkfinish = checked_add(checked_add(chunkshell, countshell),
        checked_add(vectorbytes(Vector{AbstractVector}, capacity),
        checked_add(vectorbytes(Vector{AbstractVector}, n), vectorbytes(Int, capacity))))
    countfinish = checked_add(countshell,
        checked_add(vectorbytes(Vector{AbstractVector}, n),
        checked_add(vectorbytes(Int, capacity), vectorbytes(Int, n))))
    return max(chunkfinish, countfinish)
end

"The §4.9 estimate of one record-root datum plus its column-consumer payload beyond the chunk slots."
function estimaterootrecord(p::WRecord, pf::TablePreflight, x, budget::Budget,
                            diagnostics::Union{Nothing,Encoder}=nothing)
    estimate, payload = estimaterecordvalue(p, x, pf.cols, budget,
                                            diagnostics)
    return (checked_add(planrootslot(p), estimate.retained),
            estimate.peakextra, estimate.values, payload)
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

function schemaneedsstaging(schema::Schema, budget::Budget)
    charge = vectorbytes(Bool, graphinfo(schema).nodes)
    reserve!(budget, charge)
    seen = fill(false, graphinfo(schema).nodes)
    allocated!(budget, charge)
    try
        return schemaneedsstaging(schema, seen, budget, 0)
    finally
        release!(budget, charge)
    end
end

function schemaneedsstaging(schema::Schema, seen::Vector{Bool}, budget::Budget, depth::Int)
    addresolution!(budget)
    schema isa ArraySchema && return true
    schema isa MapSchema && return true
    schema isa UnionSchema && return true
    schema isa RecordSchema && depth > 0 && return true
    id = Int(nodeid(schema)) + 1
    seen[id] && return false
    seen[id] = true
    if schema isa RecordSchema
        return any(field -> schemaneedsstaging(field.schema, seen, budget, depth + 1),
                   schema.fields)
    end
    return false
end

"""
    Avro.Writer(dst, schema; codec=:null, level=nothing, metadata=Dict{String,Vector{UInt8}}(),
                sync=nothing, block_bytes=64*1024, atomic=true, fsync=false, allow_invalid_names=false,
                allow_invalid_defaults=false, limits=Limits())

A container writer to a path (atomic by default: a sibling temp file renamed into place on `close`) or a
caller-owned `IO` (flushed, never closed). Datums are buffered under every reader limit and a block is
emitted at `block_bytes`, at the block caps, or on `flush`/`close`. The first failure poisons the writer,
including a rejected datum or limit failure from `push!`; `WriterClosedError` carries the original cause.
With `atomic=true`, the temp file is removed and the destination is untouched; `atomic=false` and
caller-owned streams can retain partial output. `close(w; abort=true)` discards the buffered block. The
do-block form closes on success and aborts on error; a finalizer aborts an unclosed writer without closing
caller-owned I/O.
"""
mutable struct Writer
    const sink::IO
    const path::Union{Nothing,String}
    const temppath::Union{Nothing,String}
    const schema::Schema
    const plan::WritePlan
    const wcodec::WriterCodec
    const syncmarker::NTuple{16,UInt8}
    const limits::Limits
    const budget::Budget
    const blockbytes::Int
    const atomic::Bool
    const fsync::Bool
    const ownsink::Bool
    const encoder::Encoder
    const datumencoder::Encoder
    const preflightbase::Int
    const preflight::Union{Nothing,TablePreflight}
    const stagevalues::Bool
    fasttype::Any                    # single-entry aligned-NamedTuple cache (Phase 4d)
    fastplans::Any
    pendingcount::Int
    pendingbytes::Int
    pendingpeak::Int
    pendingvalues::Int
    pendingcompare::Int
    pendingallowance::Int
    closed::Bool
    poison::Union{Nothing,Exception}
end

"The writer's 16-byte sync marker: validated caller bytes, or fresh `RandomDevice` bytes."
function writersyncmarker(sync)
    sync === nothing && return ntuple(_ -> rand(RandomDevice(), UInt8), 16)
    (sync isa AbstractVector{UInt8} && length(sync) == 16) || throw(ArgumentError("sync must be exactly 16 bytes"))
    return ntuple(i -> sync[i], 16)
end

"""
The charged header entries of a new writer: the bounded schema JSON print (work counters rebased to
the Reader's exact header work), the exact-capacity entries vector, and each retained key/value copy
reserved before it is made. Returns `(schemajson, entries)`.
"""
function symbolbytescopy(symbol::Symbol, budget::Budget)
    n = sizeof(symbol)
    charge = bytesbytes(n)
    reserve!(budget, charge)
    bytes = Vector{UInt8}(undef, n)
    allocated!(budget, charge)
    source = Base.unsafe_convert(Ptr{UInt8}, symbol)
    n > 0 && GC.@preserve symbol bytes unsafe_copyto!(pointer(bytes), source, n)
    return bytes
end

function writerheaderentries(schema::Schema, codec::Symbol, metadata, limits::Limits, budget::Budget)
    values0 = budget.values
    input0 = budget.input_bytes
    allowance0 = budget.allowance_used
    workcap0 = budget.workcap
    jw = BoundedWriter(budget, limits.max_schema_bytes)       # the schema JSON is produced charged and bounded
    seen = schemaseen(schema, budget)
    printschema(jw, schema, "", seen, false, 0)
    releaseseen!(budget, seen)
    schemajson = boundedtake!(jw)
    # Printing is bounded in this operation, but the container counters must start with the exact
    # header work that its Reader performs. Keep all printer reservations and rebase only work.
    budget.values = values0
    budget.input_bytes = input0
    budget.allowance_used = allowance0
    budget.workcap = workcap0
    nentries = 2 + length(metadata)
    nentries <= limits.max_metadata_entries || throw(LimitError(:max_metadata_entries, nentries, limits.max_metadata_entries, :max_metadata_entries, :encode))
    reserve!(budget, STORAGE[].vector + 16 * nentries)        # the entries vector at exact capacity (§4.4 growth rule)
    entries = Vector{Tuple{String,Vector{UInt8}}}(undef, nentries)
    allocated!(budget, STORAGE[].vector + 16 * nentries)
    reserve!(budget, stringbytes(11) + bytesbytes(sizeof(schemajson)))
    entries[1] = ("avro.schema", Vector{UInt8}(codeunits(schemajson)))
    allocated!(budget, stringbytes(11) + bytesbytes(sizeof(schemajson)))
    codecbytes = symbolbytescopy(codec, budget)
    reserve!(budget, stringbytes(10))
    entries[2] = ("avro.codec", codecbytes)
    allocated!(budget, stringbytes(10))
    i = 2
    total = checked_add(checked_add(11, sizeof(schemajson)),
                        checked_add(10, sizeof(codec)))
    total <= limits.max_metadata_bytes ||
        throw(LimitError(:max_metadata_bytes, total, limits.max_metadata_bytes,
                         :max_metadata_bytes, :encode))
    for (k, v) in metadata
        startswith(k, "avro.") && throw(ArgumentError("metadata keys in the avro.* namespace are reserved; avro.schema and avro.codec come from the constructor"))
        isstrictutf8(k) || throw(ArgumentError("metadata keys must be valid UTF-8"))
        total = checked_add(total, checked_add(sizeof(k), length(v)))
        total <= limits.max_metadata_bytes ||
            throw(LimitError(:max_metadata_bytes, total, limits.max_metadata_bytes,
                             :max_metadata_bytes, :encode))
        keycopy = ownedstringcopy(k, budget)
        valuecharge = bytesbytes(length(v))
        reserve!(budget, valuecharge)
        valuecopy = Vector{UInt8}(v)
        allocated!(budget, valuecharge)
        i += 1
        entries[i] = (keycopy, valuecopy)
    end
    beginworkdefer!(budget)
    complete = false
    try
        for _ in entries
            countvalues!(budget)                              # the Reader counts each metadata entry
        end
        addinput!(budget, total)                              # the Reader's metadata key/value denominator
        complete = true
    finally
        endworkdefer!(budget)
    end
    if complete
        checkworkscope!(budget, length(entries), total)
        checkoperationwork!(budget)
    end
    return (schemajson, entries)
end

"""
The reader's construction retention, preflighted under the writer's budget (plan §4.4): the parsed
schema graph a reader builds from the same JSON, the materialised metadata of a stream reader (key
and value buffers plus the retained entries and map), and the generic read and datum-span plans. Returns
`(preflightbase, preflight)`.
"""
function writerpreflight(schemajson::String, entries, plan, limits::Limits, budget::Budget;
                         allow_invalid_names::Bool, allow_invalid_defaults::Bool)
    base0 = budget.reserved
    pfschema = parseschema(schemajson; allow_invalid_names=allow_invalid_names, allow_invalid_defaults=allow_invalid_defaults,
                           limits=limits, budget=budget)
    reserve!(budget, 2 * STORAGE[].vector + 16 * length(entries))   # the mirror key/value vectors, exact capacity
    pfkeys = Vector{String}(undef, length(entries))
    pfvals = Vector{Vector{UInt8}}(undef, length(entries))
    allocated!(budget, 2 * STORAGE[].vector + 16 * length(entries))
    for (j, (k, v)) in enumerate(entries)
        reserve!(budget, checked_add(stringbytes(sizeof(k)), bytesbytes(length(v))))  # the retained metadata entry (byte and stream readers now charge identically)
        pfkeys[j] = k
        pfvals[j] = v
    end
    buildmap(Vector{UInt8}, pfkeys, pfvals, budget)
    pfreadplan = readplan(pfschema; budget=budget)
    spanplan(pfschema; budget=budget)
    preflightbase = budget.reserved - base0
    preflight = pfschema isa RecordSchema ?
        TablePreflight(pfschema, plan::WRecord, pfreadplan::RecordPlan,
                       budget) : nothing
    return (preflightbase, preflight)
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

"The writer's destination: `(sink, temppath, ownsink)` — a temp file first when writing atomically."
function openwritersink(dst::Union{AbstractString,IO}, atomic::Bool)
    path = dst isa AbstractString ? String(dst) : nothing
    path === nothing && return (dst, nothing, false)
    if atomic
        t, tio = mktemp(dirname(abspath(path)))
        return (tio, t, true)
    end
    return (open(path, "w"), nothing, true)
end

function Writer(dst::Union{AbstractString,IO}, schema::Schema; codec::Symbol=:null, level=nothing,
                metadata=Dict{String,Vector{UInt8}}(), sync=nothing, block_bytes::Integer=64 * 1024,
                atomic::Bool=true, fsync::Bool=false, allow_invalid_names::Bool=false,
                allow_invalid_defaults::Bool=false, limits::Limits=Limits())
    0 < block_bytes <= limits.max_block_bytes || throw(ArgumentError("block_bytes must be in 1:$(limits.max_block_bytes), got $block_bytes"))
    gi = graphinfo(schema)
    gi.repaired_names && !allow_invalid_names && throw(ArgumentError("the schema contains invalid names; pass allow_invalid_names=true to write it"))
    gi.repaired_defaults && !allow_invalid_defaults && throw(ArgumentError("the schema contains invalid defaults; pass allow_invalid_defaults=true to write it"))
    syncmarker = writersyncmarker(sync)
    metadata isa AbstractDict{<:AbstractString,<:AbstractVector{UInt8}} || throw(ArgumentError("metadata must map strings to byte vectors"))
    budget = Budget(limits; direction=:encode)                    # the budget exists before any header allocation (R04)
    wcodec = writercodec(codec, level, limits)
    sink = nothing
    temppath = nothing
    ownsink = false
    writer = nothing
    w = try
        reserve!(budget, wcodec.workspace)
        schemajson, entries = writerheaderentries(schema, codec, metadata, limits, budget)
        plan = writeplan(schema; budget=budget)
        preflightbase, preflight = writerpreflight(schemajson, entries, plan, limits, budget;
                                                   allow_invalid_names=allow_invalid_names,
                                                   allow_invalid_defaults=allow_invalid_defaults)
        path = dst isa AbstractString ? String(dst) : nothing
        sink, temppath, ownsink = openwritersink(dst, atomic)
        encoder = Encoder(budget)
        datumencoder = Encoder(budget)
        stagevalues = schemaneedsstaging(schema, budget)
        writer = Writer(sink, path, atomic ? temppath : nothing, schema, plan, wcodec, syncmarker, limits, budget,
                        Int(block_bytes), atomic, fsync, ownsink, encoder, datumencoder, preflightbase,
                        preflight, stagevalues, nothing, nothing, 0, 0, 0, 0, 0, 0, false, nothing)
        try
            writecontainerheader!(sink, entries, syncmarker)
        catch e
            poison!(writer, e)
            abortcleanup(writer; suppress=true)
            rethrow()
        end
        writer
    catch
        try
            writer === nothing && sink !== nothing &&
                cleanupwritersink(sink, temppath, ownsink; suppress=true)
        finally
            close!(budget)
        end
        rethrow()
    end
    finalizer(w) do x
        if !x.closed
            x.closed = true
            try
                abortcleanup(x; suppress=true)
            finally
                close!(x.budget)
            end
        end
        return nothing
    end
    return w
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

function abortcleanup(w::Writer; suppress::Bool=false)
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
    if !w.stagevalues
        if datum isa NamedTuple && w.fasttype === typeof(datum) &&
           w.fastplans !== nothing
            nextrows = checked_add(w.budget.rows, 1)
            nextrows <= w.limits.max_rows ||
                throw(limiterror(w.budget, :max_rows, nextrows,
                                 w.limits.max_rows))
            return pushaligneddatum!(w, datum, w.fastplans, w.preflight,
                                     nextrows, 0)
        end
        return pushprepareddatum!(w, datum)
    end
    work = writerworkcheckpoint(w.budget)
    rootcharge = 0
    staged = nothing
    stagedcompare = 0
    beginworkdefer!(w.budget)
    prepared = false
    try
        if w.stagevalues
            reset!(w.datumencoder)
            datum, rootcharge, staged = withencoderroot!(w.datumencoder, w.schema) do
                preparedvalue = datum
                charge = 0
                if w.plan isa WRecord && !(preparedvalue isa NamedTuple)
                    preparedvalue, charge = preparewriterrecord(w.plan,
                        preparedvalue, w.budget, w.datumencoder)
                end
                stagedvalue = stagewriterdatum(w.plan, preparedvalue,
                                                w.budget, w.datumencoder)
                return (preparedvalue, charge, stagedvalue)
            end
        end
        value = staged === nothing ? datum : staged.value
        stagedcompare = w.budget.compare_bytes - work[3]
        prepared = true
    finally
        endworkdefer!(w.budget)
        restorewriterwork!(w.budget, work)
        if !prepared
            staged === nothing || release!(w.budget, staged.charge)
            rootcharge > 0 && release!(w.budget, rootcharge)
        end
    end
    try
        value = staged === nothing ? datum : staged.value
        return pushprepareddatum!(w, value, stagedcompare)
    finally
        staged === nothing || release!(w.budget, staged.charge)
        rootcharge > 0 && release!(w.budget, rootcharge)
    end
end

struct StagedWriterValue
    value::Any
    charge::Int
end

struct StagedWriterMap <: AbstractDict{Any,Any}
    keys::Vector{Any}
    vals::Vector{Any}
end

function Base.length(map::StagedWriterMap)
    return length(map.keys)
end

function Base.iterate(map::StagedWriterMap, index::Int=1)
    index > length(map.keys) && return nothing
    return (map.keys[index] => map.vals[index], index + 1)
end

function Base.getindex(map::StagedWriterMap, key)
    for i in eachindex(map.keys)
        isequal(map.keys[i], key) && return map.vals[i]
    end
    throw(KeyError(key))
end

mutable struct WriterStageState
    const budget::Budget
    values::Int
    const diagnostics::Union{Nothing,Encoder}
end

function WriterStageState(budget::Budget, values::Int)
    return WriterStageState(budget, values, nothing)
end

function countstaged!(state::WriterStageState)
    state.values = checkedvalueadd(state.budget, state.values, 1)
    checkedvalueadd(state.budget, state.budget.values, state.values)
    return nothing
end

"Materialise each one-shot array, and only the enclosing composite path, once under the writer budget."
function stagewriterdatum(plan::WritePlan, datum, budget::Budget,
                          diagnostics::Union{Nothing,Encoder}=nothing)
    checkpoint = budgetcheckpoint(budget)
    value = nothing
    try
        value, changed = stagewritervalue(plan, datum,
            WriterStageState(budget, 0, diagnostics), 0)
        changed || return nothing
        charge = budget.reserved - checkpoint[1]
        return StagedWriterValue(value, charge)
    catch
        value = nothing
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

function withwriterstagepath(f::F, state::WriterStageState, schema::Schema,
                             kind::UInt8,
                             name::Union{Nothing,AbstractString,Symbol}=nothing,
                             index::Int=0) where {F}
    diagnostics = state.diagnostics
    diagnostics === nothing && return f()
    return withencodepath!(f, diagnostics, schema, kind, name, index)
end

function withwriterstageschema(f::F, state::WriterStageState,
                               schema::Schema) where {F}
    diagnostics = state.diagnostics
    diagnostics === nothing && return f()
    return withencodeschema!(f, diagnostics, schema)
end

function stagewritervalue(plan::WritePlan, value, state::WriterStageState, depth::Int)
    countstaged!(state)
    if plan isa Union{WArray,WMap,WRecord}
        nested = checked_add(depth, 1)
        checkdepth(state.budget, nested)
        plan isa WArray && return stagewriterarray(plan, value, state, nested)
        plan isa WMap && return stagewritermap(plan, value, state, nested)
        return stagewriterrecord(plan, value, state, nested)
    end
    plan isa WUnion && return stagewriterunion(plan, value, state, depth)
    return (value, false)
end

function pushstaged!(values::BuildBuf{Any}, value, state::WriterStageState)
    box = isbits(value) ? boxbytes(typeof(value)) : 0
    box > 0 && reserve!(state.budget, box)
    push!(values, state.budget, value)
    box > 0 && allocated!(state.budget, box)
    return nothing
end

function materializewriterarray(plan::WArray, items, state::WriterStageState, depth::Int,
                                declared::Union{Nothing,Int})
    capacity = declared === nothing ? 0 : min(declared, 1024)
    values = BuildBuf{Any}(state.budget, capacity)
    count = 0
    for item in items
        count = checked_add(count, 1)
        checkestimatedcount(state.budget, count)
        staged, _ = withwriterstagepath(state, plan.schema.items,
                                        ENCODE_PATH_INDEX, nothing,
                                        count - 1) do
            stagewritervalue(plan.items, item, state, depth)
        end
        pushstaged!(values, staged, state)
    end
    declared === nothing || count == declared || encodeerror("array size changed during iteration", items)
    return (finishbuild!(values, state.budget), true)
end

function stagewriterarray(plan::WArray, value, state::WriterStageState, depth::Int)
    items = arrayitems(value)
    sized = Base.IteratorSize(typeof(items)) isa Union{Base.HasLength,Base.HasShape}
    if !sized
        return materializewriterarray(plan, items, state, depth, nothing)
    end
    count = length(items)
    checkestimatedcount(state.budget, count)
    repeatable = items isa Union{Vector,Tuple}
    repeatable || return materializewriterarray(plan, items, state, depth, count)
    plan.items isa Union{WArray,WMap,WRecord,WUnion} || return (value, false)
    output = nothing
    index = 0
    for item in items
        index = checked_add(index, 1)
        index <= count || encodeerror("array size changed during iteration", value)
        staged, changed = withwriterstagepath(state, plan.schema.items,
                                              ENCODE_PATH_INDEX, nothing,
                                              index - 1) do
            stagewritervalue(plan.items, item, state, depth)
        end
        if changed && output === nothing
            output = BuildBuf{Any}(state.budget, min(count, 1024))
            for previous in 1:index - 1
                pushstaged!(output, items[previous], state)
            end
        end
        output === nothing || pushstaged!(output, staged, state)
    end
    index == count || encodeerror("array size changed during iteration", value)
    output === nothing && return (value, false)
    return (finishbuild!(output, state.budget), true)
end

function stagewritermap(plan::WMap, value, state::WriterStageState, depth::Int)
    pairs = mappairs(value)
    count = length(pairs)
    checkestimatedcount(state.budget, count)
    keys = BuildBuf{Any}(state.budget, min(count, 1024))
    vals = BuildBuf{Any}(state.budget, min(count, 1024))
    seen = 0
    for (key, item) in pairs
        seen = checked_add(seen, 1)
        seen <= count || encodeerror("map size changed during iteration", value)
        staged, _ = if key isa Union{AbstractString,Symbol}
            withwriterstagepath(state, plan.schema.values,
                                ENCODE_PATH_KEY, key) do
                stagewritervalue(plan.values, item, state, depth)
            end
        else
            stagewritervalue(plan.values, item, state, depth)
        end
        pushstaged!(keys, key, state)
        pushstaged!(vals, staged, state)
    end
    seen == count || encodeerror("map size changed during iteration", value)
    outkeys = finishbuild!(keys, state.budget)
    outvals = finishbuild!(vals, state.budget)
    charge = shellbytes(StagedWriterMap)
    reserve!(state.budget, charge)
    result = StagedWriterMap(outkeys, outvals)
    allocated!(state.budget, charge)
    return (result, true)
end

function stagedrecordfield(plan::WRecord, value::Record, index::Int, budget::Budget)
    source = getfield(value, :schema)
    values = getfield(value, :values)
    source === plan.schema && return values[index]
    target = plan.schema.fields[index].name
    position = budgetedget(source.fieldindex, target, 0, budget)
    position == 0 && encodeerror("record value lacks field \"$target\"", value)
    return values[position]
end

function stagedrecordfield(plan::WRecord, value, index::Int, budget::Budget)
    return recordfieldvalue(plan, value, index, budget)
end

function assignstagedrecord!(values::Vector{Any}, index::Int, value, budget::Budget)
    box = isbits(value) ? boxbytes(typeof(value)) : 0
    box > 0 && reserve!(budget, box)
    values[index] = value
    box > 0 && allocated!(budget, box)
    return nothing
end

function stagewriterrecord(plan::WRecord, value, state::WriterStageState, depth::Int)
    count = length(plan.fields)
    aligned = value isa Record && getfield(value, :schema) === plan.schema
    normalized = false
    preparedcharge = 0
    if depth > 1 && !aligned
        value, preparedcharge = preparewriterrecord(plan, value, state.budget,
                                                    state.diagnostics)
        aligned = true
        normalized = true
    end
    output = nothing
    for (index, field) in enumerate(plan.fields)
        schemafield = plan.schema.fields[index]
        staged, changed = withwriterstagepath(state, schemafield.schema,
                                              ENCODE_PATH_FIELD,
                                              schemafield.name) do
            original = stagedrecordfield(plan, value, index, state.budget)
            stagewritervalue(field, original, state, depth)
        end
        if changed && output === nothing
            reserve!(state.budget, recordbytes(count))
            output = Vector{Any}(undef, count)
            allocated!(state.budget, vectorbytes(Any, count))
            for previous in 1:index - 1
                assignstagedrecord!(output, previous,
                    stagedrecordfield(plan, value, previous, state.budget), state.budget)
            end
        end
        output === nothing || assignstagedrecord!(output, index, staged, state.budget)
    end
    output === nothing && return (value, normalized)
    result = Record(plan.schema, output, Val(:unchecked))
    allocated!(state.budget, recordbytes(count) - vectorbytes(Any, count))
    preparedcharge > 0 && release!(state.budget, preparedcharge)
    return (result, true)
end

function stagewriterunion(plan::WUnion, value, state::WriterStageState, depth::Int)
    if value isa Base.Enum
        index, symbol = selectenumbranch(plan, value, state.budget)
        owned = ownedstringcopy(symbol, state.budget)
        staged, _ = withwriterstageschema(state,
                                          plan.schema.branches[index]) do
            stagewritervalue(plan.branches[index], owned, state, depth)
        end
        charge = unionvaluebytes()
        reserve!(state.budget, charge)
        result = UnionValue(index, staged)
        allocated!(state.budget, charge)
        return (result, true)
    end
    index = selectbranch(plan, value, state.budget)
    inner = value isa UnionValue ? value.value : value
    staged, changed = withwriterstageschema(state,
                                            plan.schema.branches[index]) do
        stagewritervalue(plan.branches[index], inner, state, depth)
    end
    value isa UnionValue && !changed && return (value, false)
    box = isbits(staged) ? boxbytes(typeof(staged)) : 0
    charge = checked_add(unionvaluebytes(), box)
    reserve!(state.budget, charge)
    result = UnionValue(index, staged)
    allocated!(state.budget, charge)
    return (result, true)
end

function withwriterpreparepath(f::F, diagnostics::Union{Nothing,Encoder},
                               field::Field) where {F}
    diagnostics === nothing && return f()
    return withencodepath!(f, diagnostics, field.schema, ENCODE_PATH_FIELD,
                           field.name)
end

function preparedrecord(p::WRecord, values::Vector{Any}, boxes::Int,
                        budget::Budget,
                        diagnostics::Union{Nothing,Encoder}=nothing)
    for (i, value) in enumerate(values)
        if value === DICT_MISSING
            field = p.schema.fields[i]
            withwriterpreparepath(diagnostics, field) do
                encodeerror("record value lacks field \"$(field.name)\"", values)
            end
        end
    end
    reserve!(budget, STORAGE[].record)
    record = Record(p.schema, values, Val(:unchecked))
    allocated!(budget, STORAGE[].record)
    return (record, checked_add(recordbytes(length(values)), boxes))
end

function preparedrecordstorage(p::WRecord, budget::Budget)
    n = length(p.fields)
    reserve!(budget, vectorbytes(Any, n))
    values = Vector{Any}(undef, n)
    allocated!(budget, vectorbytes(Any, n))
    fill!(values, DICT_MISSING)
    return values
end

function assignpreparedrecord!(values::Vector{Any}, index::Int, value, budget::Budget)
    box = isbits(value) ? boxbytes(typeof(value)) : 0
    box > 0 && reserve!(budget, box)
    values[index] = value
    box > 0 && allocated!(budget, box)
    return box
end

function preparewriterrecord(p::WRecord, value::Record, budget::Budget,
                             diagnostics::Union{Nothing,Encoder}=nothing)
    source = getfield(value, :schema)
    source === p.schema && return (value, 0)
    sourcevalues = getfield(value, :values)
    length(sourcevalues) == length(source.fields) ||
        encodeerror("record value has $(length(sourcevalues)) values for $(length(source.fields)) fields", value)
    checkpoint = budgetcheckpoint(budget)
    values = nothing
    try
        values = preparedrecordstorage(p, budget)
        boxes = 0
        for (i, field) in enumerate(p.schema.fields)
            box = withwriterpreparepath(diagnostics, field) do
                sourceindex = budgetedget(source.fieldindex, field.name, 0,
                                          budget)
                sourceindex == 0 &&
                    encodeerror("record value lacks field \"$(field.name)\"", value)
                assignpreparedrecord!(values, i, sourcevalues[sourceindex],
                                      budget)
            end
            boxes = checked_add(boxes, box)
        end
        return preparedrecord(p, values, boxes, budget, diagnostics)
    catch
        values = nothing
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

function preparewriterrecord(p::WRecord, value::AbstractDict, budget::Budget,
                             diagnostics::Union{Nothing,Encoder}=nothing)
    checkpoint = budgetcheckpoint(budget)
    values = nothing
    try
        values, boxes = recorddictvalues(p, value, budget)
        return preparedrecord(p, values, boxes, budget, diagnostics)
    catch
        values = nothing
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

function preparewriterrow(p::WRecord, row, budget::Budget,
                          diagnostics::Union{Nothing,Encoder}=nothing)
    checkpoint = budgetcheckpoint(budget)
    positions = nothing
    values = nothing
    try
        positions = recordrowpositions(p, row, budget)
        values = preparedrecordstorage(p, budget)
        boxes = 0
        for i in eachindex(p.fields)
            field = p.schema.fields[i]
            box = withwriterpreparepath(diagnostics, field) do
                positions[i] == 0 &&
                    encodeerror("row lacks column \"$(field.name)\"", row)
                assignpreparedrecord!(values, i,
                    Tables.getcolumn(row, positions[i]), budget)
            end
            boxes = checked_add(boxes, box)
        end
        release!(budget, vectorbytes(Int, length(positions)))
        positions = nothing
        return preparedrecord(p, values, boxes, budget, diagnostics)
    catch
        positions = values = nothing
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

function preparewriterrecord(p::WRecord, value::TableRow, budget::Budget,
                             diagnostics::Union{Nothing,Encoder}=nothing)
    return preparewriterrow(p, value.row, budget, diagnostics)
end

function preparewriterrecord(p::WRecord, value::Tables.AbstractRow,
                             budget::Budget,
                             diagnostics::Union{Nothing,Encoder}=nothing)
    return preparewriterrow(p, value, budget, diagnostics)
end

function preparewriterrecord(p::WRecord, value::T, budget::Budget,
                             diagnostics::Union{Nothing,Encoder}=nothing) where {T}
    valid = value isa NamedTuple ||
        (isstructtype(T) && !(value isa AbstractArray) && !(value isa AbstractString))
    valid || encodeerror("expected a record value for $(fullname(p.schema))", value)
    checkpoint = budgetcheckpoint(budget)
    positions = nothing
    values = nothing
    try
        positions = recordfieldpositions(p, T, budget)
        values = preparedrecordstorage(p, budget)
        boxes = 0
        for i in eachindex(p.fields)
            field = p.schema.fields[i]
            box = withwriterpreparepath(diagnostics, field) do
                positions[i] == 0 &&
                    encodeerror("$(T) has no field for \"$(field.name)\" of record $(fullname(p.schema))", value)
                assignpreparedrecord!(values, i,
                                      getfield(value, positions[i]), budget)
            end
            boxes = checked_add(boxes, box)
        end
        release!(budget, vectorbytes(Int, length(positions)))
        positions = nothing
        return preparedrecord(p, values, boxes, budget, diagnostics)
    catch
        positions = values = nothing
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

"The work counters that one staged Writer datum must restore before it joins a block."
function writerworkcheckpoint(budget::Budget)
    return (budget.values, budget.input_bytes, budget.compare_bytes,
            budget.allowance_used, budget.workcap, budget.workdeferred)
end

function restorewriterwork!(budget::Budget, checkpoint::NTuple{6,Int})
    budget.values = checkpoint[1]
    budget.input_bytes = checkpoint[2]
    budget.compare_bytes = checkpoint[3]
    budget.allowance_used = checkpoint[4]
    budget.workcap = checkpoint[5]
    budget.workdeferred = checkpoint[6]
    return nothing
end

"Whether a pending block is safe under the one-byte lower bound of its size varint."
function writercandidatefits(w::Writer, count::Int, payload::Int, values::Int,
                             compared::Int, sizevarint::Int=1)
    framing = checked_add(checked_add(varintlength(count), sizevarint), 16)
    blockinput = checked_add(payload, framing)
    values < typemax(Int) || return false
    blockvalues = values + 1                            # one emitted codec member
    blockvalues <= muladdcap(w.limits.max_values_per_byte, blockinput,
                             w.limits.work_allowance) || return false
    blockvalues <= w.limits.max_total_values - w.budget.values || return false
    totalvalues = w.budget.values + blockvalues
    totalinput = checked_add(w.budget.input_bytes, blockinput)
    totalvalues <= muladdcap(w.limits.max_values_per_byte, totalinput,
                             w.limits.work_allowance) || return false
    totalcompare = checked_add(w.budget.compare_bytes, compared)
    totalcompare <= muladdcap(w.limits.max_compare_bytes_per_byte,
                              totalinput, w.limits.work_allowance) ||
        return false
    return true
end

@noinline function diagnosealignedestimate!(w::Writer, plans::Tuple,
                                            datum::NamedTuple,
                                            pf::Union{Nothing,TablePreflight})
    checkpoint = writerworkcheckpoint(w.budget)
    reset!(w.datumencoder)
    beginworkdefer!(w.budget)
    try
        return withencoderroot!(w.datumencoder, w.schema) do
            estimatealigned(plans, datum, pf === nothing ? nothing : pf.cols,
                            w.budget, (w.schema::RecordSchema).fields,
                            w.datumencoder)
        end
    finally
        endworkdefer!(w.budget)
        restorewriterwork!(w.budget, checkpoint)
        reset!(w.datumencoder)
    end
end

@noinline function diagnosealignedencode!(w::Writer, plans::Tuple,
                                          datum::NamedTuple)
    checkpoint = writerworkcheckpoint(w.budget)
    reset!(w.datumencoder)
    beginworkdefer!(w.budget)
    try
        return encodealigned!(plans, w.datumencoder, datum,
                              w.schema::RecordSchema)
    finally
        endworkdefer!(w.budget)
        restorewriterwork!(w.budget, checkpoint)
        reset!(w.datumencoder)
    end
end

"Estimate and append one aligned row while its work counters remain deferred."
@inline function preparealigneddatum!(w::Writer, datum::NamedTuple, plans::Tuple,
                                      pf::Union{Nothing,TablePreflight},
                                      stagedcompare::Int)
    checkpoint = writerworkcheckpoint(w.budget)
    start = w.encoder.pos
    credited = w.encoder.credited
    complete = false
    beginworkdefer!(w.budget)
    try
        eb, ep, ev, pp = estimatealigned(plans, datum,
                                         pf === nothing ? nothing : pf.cols,
                                         w.budget)
        eb <= w.limits.max_block_output_bytes ||
            throw(LimitError(:max_block_output_bytes, eb,
                             w.limits.max_block_output_bytes,
                             :max_block_output_bytes, :encode))
        # Pending block bytes are accounted when the block is committed. Mark the
        # existing prefix as settled so `encodedwork!` measures only this datum.
        w.encoder.credited = start
        encodealignedfast!(plans, w.encoder, datum)
        datumbytes = w.encoder.pos - start
        datumvalues = w.budget.values - checkpoint[1]
        datumcompare = checked_add(stagedcompare,
                                   w.budget.compare_bytes - checkpoint[3])
        datumvalues == ev ||
            throw(ArgumentError("internal error: datum estimate counted $ev values but encoding counted $datumvalues"))
        datumbytes <= w.limits.max_block_bytes ||
            throw(LimitError(:max_block_bytes, datumbytes,
                             w.limits.max_block_bytes,
                             :max_block_bytes, :encode))
        complete = true
        return (eb, ep, pp, datumbytes, datumvalues, datumcompare)
    finally
        endworkdefer!(w.budget)
        restorewriterwork!(w.budget, checkpoint)
        if !complete
            w.encoder.pos = start
            w.encoder.credited = credited
        end
    end
end

"The aligned NamedTuple path encodes in place and stages bytes only at a block boundary."
@inline function pushaligneddatum!(w::Writer, datum::NamedTuple, plans::Tuple,
                                   pf::Union{Nothing,TablePreflight}, nextrows::Int,
                                   stagedcompare::Int)
    start = w.encoder.pos
    credited = w.encoder.credited
    eb, ep, pp, datumbytes, datumvalues, datumcompare = try
        preparealigneddatum!(w, datum, plans, pf, stagedcompare)
    catch err
        if err isa EncodeError
            isempty(err.path) ? diagnosealignedestimate!(w, plans, datum, pf) :
                                diagnosealignedencode!(w, plans, datum)
        end
        rethrow()
    end

    nextcount = nextpayload = nextoutput = nextvalues = nextcompare = 0
    lowerfits = false
    try
        nextcount = checked_add(w.pendingcount, 1)
        nextpayload = w.encoder.pos
        nextoutput = checked_add(w.pendingbytes, eb)
        nextvalues = satadd(w.pendingvalues, datumvalues)
        nextcompare = checked_add(w.pendingcompare, datumcompare)
        lowerfits = writercandidatefits(w, nextcount, nextpayload, nextvalues,
                                        nextcompare)
    catch
        w.encoder.pos = start
        w.encoder.credited = credited
        rethrow()
    end
    boundary = false
    if w.pendingcount > 0 &&
       (nextoutput > w.limits.max_block_output_bytes ||
        nextcount > w.limits.max_block_count ||
        nextpayload > w.limits.max_block_bytes || !lowerfits)
        reset!(w.datumencoder)
        try
            writeraw!(w.datumencoder, w.encoder.buf, start + 1, datumbytes)
        catch
            w.encoder.pos = start
            w.encoder.credited = credited
            reset!(w.datumencoder)
            rethrow()
        end
        w.encoder.pos = start
        w.encoder.credited = credited
        try
            flushblock!(w)
            writeraw!(w.encoder, w.datumencoder.buf, 1, datumbytes)
            w.encoder.credited = w.encoder.pos
        finally
            reset!(w.datumencoder)
        end
        start = 0
        credited = 0
        nextcount = 1
        nextpayload = datumbytes
        nextoutput = eb
        nextvalues = datumvalues
        nextcompare = datumcompare
        boundary = true
    end
    nextpendingpayload = 0
    try
        boundary &&
            (lowerfits = writercandidatefits(w, nextcount, nextpayload,
                                             nextvalues, nextcompare))
        nextpendingpayload = pf === nothing ? 0 :
            checked_add(pf.pendingpayload, pp)
        nextcount <= w.limits.max_block_count ||
            throw(limiterror(w.budget, :max_block_count, nextcount,
                             w.limits.max_block_count))
        nextoutput <= w.limits.max_block_output_bytes ||
            throw(LimitError(:max_block_output_bytes, nextoutput,
                             w.limits.max_block_output_bytes,
                             :max_block_output_bytes, :encode))
        nextpayload <= w.limits.max_block_bytes ||
            throw(LimitError(:max_block_bytes, nextpayload,
                             w.limits.max_block_bytes, :max_block_bytes, :encode))
    catch
        w.encoder.pos = start
        w.encoder.credited = credited
        rethrow()
    end
    w.budget.rows = nextrows
    w.pendingcount = nextcount
    w.pendingbytes = nextoutput
    w.pendingpeak = max(w.pendingpeak, ep)
    w.pendingvalues = nextvalues
    w.pendingcompare = nextcompare
    w.pendingallowance = max(w.pendingallowance,
                             workdeficit(w.budget, datumvalues, datumbytes))
    pf === nothing || (pf.pendingpayload = nextpendingpayload)
    (!lowerfits || w.encoder.pos >= w.blockbytes) && flushblock!(w)
    return w
end

function pushprepareddatum!(w::Writer, datum, stagedcompare::Int=0)
    nextrows = checked_add(w.budget.rows, 1)
    nextrows <= w.limits.max_rows || throw(limiterror(w.budget, :max_rows, nextrows, w.limits.max_rows))
    pf = w.preflight
    fp = nothing
    if datum isa NamedTuple
        if w.fasttype === typeof(datum)
            fp = w.fastplans
        else
            fp = alignedplans(w.plan, typeof(datum), w.budget)
            oldcharge = w.fastplans === nothing ? 0 : sizeof(w.fastplans)
            w.fasttype = typeof(datum)
            w.fastplans = fp
            oldcharge > 0 && release!(w.budget, oldcharge)
        end
    end
    fp !== nothing && !w.stagevalues &&
        return pushaligneddatum!(w, datum, fp, pf, nextrows, stagedcompare)
    checkpoint = writerworkcheckpoint(w.budget)
    preparedcharge = 0
    eb = ep = ev = pp = datumbytes = datumvalues = datumcompare = 0
    reset!(w.datumencoder)
    beginworkdefer!(w.budget)
    complete = false
    try
        datum, preparedcharge, eb, ep, ev, pp =
            withencoderroot!(w.datumencoder, w.schema) do
                preparedvalue = datum
                charge = 0
                if fp === nothing && w.plan isa WRecord
                    preparedvalue, charge = preparewriterrecord(w.plan,
                        preparedvalue, w.budget, w.datumencoder)
                end
                if fp !== nothing
                    estimate = estimatealigned(fp, preparedvalue,
                        pf === nothing ? nothing : pf.cols, w.budget,
                        (w.schema::RecordSchema).fields, w.datumencoder)
                    return (preparedvalue, charge, estimate...)
                elseif pf === nothing
                    output, peak, values = estimaterootvalue(w.plan,
                        preparedvalue, w.budget, w.datumencoder)
                    return (preparedvalue, charge, output, peak, values, 0)
                else
                    estimate = estimaterootrecord(w.plan::WRecord, pf,
                        preparedvalue, w.budget, w.datumencoder)
                    return (preparedvalue, charge, estimate...)
                end
            end
        eb > w.limits.max_block_output_bytes &&
            throw(LimitError(:max_block_output_bytes, eb, w.limits.max_block_output_bytes, :max_block_output_bytes, :encode))
        fp === nothing ? encodedatum!(w.plan, w.datumencoder, datum, w.schema) :
                         encodealigned!(fp, w.datumencoder, datum,
                                        w.schema::RecordSchema)
        datumbytes = w.datumencoder.pos
        datumvalues = w.budget.values - checkpoint[1]
        datumcompare = checked_add(stagedcompare,
                                   w.budget.compare_bytes - checkpoint[3])
        datumvalues == ev ||
            throw(ArgumentError("internal error: datum estimate counted $ev values but encoding counted $datumvalues"))
        datumbytes <= w.limits.max_block_bytes ||
            throw(LimitError(:max_block_bytes, datumbytes, w.limits.max_block_bytes,
                             :max_block_bytes, :encode))
        complete = true
    finally
        endworkdefer!(w.budget)
        restorewriterwork!(w.budget, checkpoint)
        if !complete
            reset!(w.datumencoder)
            preparedcharge > 0 && release!(w.budget, preparedcharge)
        end
    end
    try
        complete || return w
        nextcount = checked_add(w.pendingcount, 1)
        nextpayload = checked_add(w.encoder.pos, datumbytes)
        nextoutput = checked_add(w.pendingbytes, eb)
        nextvalues = satadd(w.pendingvalues, datumvalues)
        nextcompare = checked_add(w.pendingcompare, datumcompare)
        lowerfits = writercandidatefits(w, nextcount, nextpayload, nextvalues,
                                        nextcompare)
        if w.pendingcount > 0 &&
           (nextoutput > w.limits.max_block_output_bytes ||
            nextcount > w.limits.max_block_count ||
            nextpayload > w.limits.max_block_bytes || !lowerfits)
            flushblock!(w)
            nextcount = 1
            nextpayload = datumbytes
            nextoutput = eb
            nextvalues = datumvalues
            nextcompare = datumcompare
            lowerfits = writercandidatefits(w, nextcount, nextpayload, nextvalues,
                                            nextcompare)
        end
        nextcount <= w.limits.max_block_count ||
            throw(limiterror(w.budget, :max_block_count, nextcount,
                             w.limits.max_block_count))
        nextoutput <= w.limits.max_block_output_bytes ||
            throw(LimitError(:max_block_output_bytes, nextoutput,
                             w.limits.max_block_output_bytes,
                             :max_block_output_bytes, :encode))
        nextpayload <= w.limits.max_block_bytes ||
            throw(LimitError(:max_block_bytes, nextpayload,
                             w.limits.max_block_bytes, :max_block_bytes, :encode))
        writeraw!(w.encoder, w.datumencoder.buf, 1, datumbytes)
        addrows!(w.budget, 1)
        w.pendingcount = nextcount
        w.pendingbytes = nextoutput
        w.pendingpeak = max(w.pendingpeak, ep)
        w.pendingvalues = nextvalues
        w.pendingcompare = nextcompare
        w.pendingallowance = max(w.pendingallowance,
                                 workdeficit(w.budget, datumvalues, datumbytes))
        pf === nothing || (pf.pendingpayload = checked_add(pf.pendingpayload, pp))
        (!lowerfits || w.encoder.pos >= w.blockbytes) && flushblock!(w)
        return w
    finally
        reset!(w.datumencoder)
        preparedcharge > 0 && release!(w.budget, preparedcharge)
    end
end

function Base.write(w::Writer, datums)
    for d in datums
        push!(w, d)
    end
    return w
end

"Validate the exact work carried by one compressed block before any sink bytes change."
function writerflushprojection(w::Writer, payloadbytes::Int, compressedbytes::Int)
    framing = checked_add(checked_add(varintlength(w.pendingcount),
                                      varintlength(compressedbytes)), 16)
    blockinput = checked_add(payloadbytes, framing)
    blockvalues = checkedvalueadd(w.budget, w.pendingvalues, 1) # one codec member
    blockcap = muladdcap(w.limits.max_values_per_byte, blockinput,
                         w.limits.work_allowance)
    blockvalues <= blockcap ||
        throw(limiterror(w.budget, :max_values_per_byte, blockvalues, blockcap))
    totalvalues = checkedvalueadd(w.budget, w.budget.values, blockvalues)
    totalinput = checked_add(w.budget.input_bytes, blockinput)
    operationcap = muladdcap(w.limits.max_values_per_byte, totalinput,
                             w.limits.work_allowance)
    totalvalues <= operationcap ||
        throw(limiterror(w.budget, :max_values_per_byte, totalvalues,
                         operationcap))
    totalcompare = checked_add(w.budget.compare_bytes, w.pendingcompare)
    comparecap = muladdcap(w.limits.max_compare_bytes_per_byte,
                           totalinput, w.limits.work_allowance)
    totalcompare <= comparecap ||
        throw(limiterror(w.budget, :max_compare_bytes_per_byte, totalcompare,
                         comparecap))
    allowance = max(w.budget.allowance_used, w.pendingallowance,
                    workdeficit(w.budget, blockvalues, blockinput),
                    workdeficit(w.budget, totalvalues, totalinput))
    return (; values=totalvalues, input=totalinput, compared=totalcompare,
            allowance, framing)
end

"Commit a block's exact work counters after its complete bytes reached the sink."
function commitwriterwork!(w::Writer, projection, nextblocks::Int)
    budget = w.budget
    budget.values = projection.values
    budget.input_bytes = projection.input
    budget.compare_bytes = projection.compared
    budget.allowance_used = projection.allowance
    budget.workcap = min(w.limits.max_total_values,
                         muladdcap(w.limits.max_values_per_byte,
                                   projection.input, w.limits.work_allowance))
    budget.members = checked_add(budget.members, 1)
    budget.blocks = nextblocks
    return nothing
end

function flushblock!(w::Writer)
    w.pendingcount == 0 && return nothing
    transient = budgetcheckpoint(w.budget)
    blockbytes = nothing
    compressed = nothing
    try
        w.encoder.pos <= w.limits.max_block_bytes ||
            throw(LimitError(:max_block_bytes, w.encoder.pos, w.limits.max_block_bytes,
                             :max_block_bytes, :encode))
        nextblocks = checked_add(w.budget.blocks, 1)
        nextblocks <= w.limits.max_blocks ||
            throw(limiterror(w.budget, :max_blocks, nextblocks, w.limits.max_blocks))
        pf = w.preflight
        chunk = payload = rows = 0
        tableprojected = directbase = streambase = 0
        if pf !== nothing
            # The complete mapped and streamed Table peaks with this block included: retained value
            # payload once, exact column/chunk vectors, construction scratch, and final outer state.
            n = w.pendingcount
            chunk = pf.chunkbytes
            finals = 0
            rows = pf.rows + n
            ncols = length(pf.cols)
            nblocks = pf.nblocks + 1
            currentcolumns = 0
            for E in pf.cols
                currentcolumn = vectorbytes(E, n)
                currentcolumns = checked_add(currentcolumns, currentcolumn)
                chunk = checked_add(chunk, currentcolumn)
                finals = checked_add(finals, vectorbytes(E, rows))
            end
            chunk = checked_add(chunk, vectorbytes(AbstractVector, ncols))
            payload = checked_add(pf.payload, pf.pendingpayload)
            finalouter = vectorbytes(AbstractVector, ncols)
            counts = vectorbytes(Int, nblocks)
            coltypes = vectorbytes(Type, ncols)
            kept = vectorbytes(Int, ncols)
            chunkindex = vectorbytes(Vector{AbstractVector}, nblocks)
            assembly = checked_add(w.preflightbase,
                checked_add(payload,
                checked_add(chunk,
                checked_add(finals,
                checked_add(finalouter,
                checked_add(coltypes,
                checked_add(kept, checked_add(counts, chunkindex))))))))
            streamfinish = checked_add(w.preflightbase,
                checked_add(payload,
                checked_add(chunk,
                checked_add(coltypes,
                checked_add(kept, tablebuildfinishpeak(nblocks))))))
            streampush = checked_add(w.preflightbase,
                checked_add(payload,
                checked_add(chunk,
                checked_add(coltypes,
                checked_add(kept,
                checked_add(pf.builderstate, tablebuildpushpeak(nblocks)))))))
            finalstate = checked_add(w.preflightbase,
                checked_add(payload,
                checked_add(finals,
                checked_add(tableouterstate(ncols, nblocks),
                            checked_add(coltypes, checked_add(kept, counts))))))
            direct = checked_add(w.preflightbase,
                checked_add(payload,
                checked_add(finals,
                checked_add(finalouter,
                checked_add(coltypes,
                checked_add(kept,
                checked_add(counts,
                checked_add(pf.builderstate,
                            blocktablecharge(nextpow2rows(nblocks))))))))))
            tableprojected = max(assembly, streamfinish, streampush, finalstate, direct)
            directbase = checked_add(w.preflightbase,
                checked_add(pf.payload,
                checked_add(finals,
                checked_add(finalouter,
                checked_add(coltypes,
                checked_add(kept,
                checked_add(pf.builderstate,
                            blocktablecharge(nextpow2rows(nblocks)))))))))
            streambase = checked_add(w.preflightbase,
                checked_add(pf.payload,
                checked_add(pf.chunkbytes,
                checked_add(currentcolumns,
                checked_add(coltypes,
                checked_add(kept,
                checked_add(pf.builderstate, tablebuildstate(pf.nblocks))))))))
            tableprojected <= w.budget.ceiling ||
                throw(LimitError(:max_total_bytes, tableprojected, w.budget.ceiling,
                                 :max_total_bytes, :encode))
        end
        reserve!(w.budget, bytesbytes(w.encoder.pos))              # the pending-block copy, before take! (R04)
        blockbytes = take!(w.encoder)
        allocated!(w.budget, bytesbytes(length(blockbytes)))
        cbound = compressbound(w.wcodec, length(blockbytes))
        reserve!(w.budget, cbound)                                  # the compressor's output bound, before it allocates
        compressed = compressblock(w.wcodec, blockbytes, w.budget)
        length(compressed) <= w.limits.max_block_bytes ||
            throw(LimitError(:max_block_bytes, length(compressed), w.limits.max_block_bytes, :max_block_bytes, :encode))
        verifyframe(w.wcodec, compressed, w.limits)
        projection = writerflushprojection(w, length(blockbytes), length(compressed))
        outputpeak = checked_add(w.pendingbytes, w.pendingpeak)
        peak = readerblockpeak(length(compressed), length(blockbytes), outputpeak,
                               w.wcodec.name === :null ? 0 : w.limits.max_codec_memory, w.wcodec.name === :null)
        if pf !== nothing
            tableoutput = checked_add(pf.pendingpayload, w.pendingpeak)
            tableblock = readerblockpeak(length(compressed), length(blockbytes), tableoutput,
                                         w.wcodec.name === :null ? 0 : w.limits.max_codec_memory,
                                         w.wcodec.name === :null)
            tableprojected = max(tableprojected, checked_add(directbase, tableblock),
                                 checked_add(streambase, tableblock))
            tableprojected <= w.budget.ceiling ||
                throw(LimitError(:max_total_bytes, tableprojected, w.budget.ceiling,
                                 :max_total_bytes, :encode))
        end
        reserve!(w.budget, peak)                       # one block's transient reader peak fits the ceiling
        unreserve!(w.budget, peak)                     # a pure admission probe; nothing was allocated
        writevarint(w.sink, w.pendingcount)
        writevarint(w.sink, length(compressed))
        Base.write(w.sink, compressed)
        for b in w.syncmarker
            Base.write(w.sink, b)
        end
        commitwriterwork!(w, projection, nextblocks)
        if pf !== nothing
            pf.chunkbytes = chunk
            pf.payload = payload
            pf.rows = rows
            pf.nblocks += 1
            pf.pendingpayload = 0
        end
    catch e
        poison!(w, e)
        rethrow()
    finally
        blockbytes = nothing
        compressed = nothing
        rollbackreservations!(w.budget, transient)
    end
    w.pendingcount = 0
    w.pendingbytes = 0
    w.pendingpeak = 0
    w.pendingvalues = 0
    w.pendingcompare = 0
    w.pendingallowance = 0
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
        ccall(:_commit, Cint, (Cint,), fd(io))
    else
        ccall(:fsync, Cint, (Cint,), fd(io))
    end
    return nothing
end

function Base.close(w::Writer; abort::Bool=false)
    w.closed && return nothing
    if abort || w.poison !== nothing
        w.closed = true
        try
            abortcleanup(w)
        finally
            close!(w.budget)
        end
        return nothing
    end
    try
        flushblock!(w)
        w.fsync ? syncfile(w.sink) : flush(w.sink)
        if w.path !== nothing
            w.ownsink && close(w.sink)
            w.temppath !== nothing && Base.Filesystem.rename(w.temppath, w.path)
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
    finally
        close!(w.budget)
    end
    return nothing
end

# ---- Avro.write / tobuffer --------------------------------------------------------------------------

"""
    Avro.write(dst, table; schema=nothing, codec=:null, level=nothing, metadata=Dict(), block_bytes=64*1024,
               name="Record", namespace="", sync=nothing, atomic=true, fsync=false,
               allow_invalid_names=false, allow_invalid_defaults=false, limits=Limits()) -> dst

Write a Tables.jl source as a container file. Schema precedence: an explicit `schema=`, else the
conventional derivation from `Tables.schema` (a source reporting none raises an `ArgumentError` showing
how to supply one). `Tables.partitions` become block boundaries (each non-empty partition ends a block).
"""
function write(dst::Union{AbstractString,IO}, table; schema::Union{Nothing,Schema}=nothing,
               name::AbstractString="Record", namespace::AbstractString="", limits::Limits=Limits(), kw...)
    (table isa Union{Rows,Tables.Partitioner} || Tables.istable(typeof(table))) ||
        throw(ArgumentError("`Avro.write(dst, x)` writes Tables.jl sources; use `Avro.encode!` to write one datum"))
    s = schema
    s === nothing && (s = retainedschema(table))
    if table isa Rows && getfield(table, :mode) !== :generic
        w = Writer(dst, s; limits=limits, kw...)                # typed and non-record rows write datum-wise
        ok = false
        try
            for v in table
                push!(w, v)
            end
            ok = true
        finally
            ok ? close(w) : close(w; abort=true)
        end
        return dst
    end
    w = nothing
    ok = false
    try
        for part in Tables.partitions(table)
            rows = Tables.rows(part)
            if s === nothing
                ts = Tables.schema(rows)
                ts === nothing && throw(ArgumentError("the source reports no Tables.schema and no `schema=` was given; materialise it (`Tables.dictrowtable`) and derive one with `Avro.schema(Tables.schema(...))`"))
                s = Avro.schema(ts; name=name, namespace=namespace, limits=limits)
            end
            w === nothing && (w = Writer(dst, s; limits=limits, kw...))
            for row in rows
                push!(w, row isa Union{NamedTuple,Record,AbstractDict,Tables.AbstractRow} ? row : TableRow(row))
            end
            flushblock!(w)
        end
        if w === nothing
            s === nothing && throw(ArgumentError("an empty partitioned source reports no Tables.schema; pass `schema=`"))
            w = Writer(dst, s; limits=limits, kw...)
        end
        ok = true
    finally
        w === nothing || (ok ? close(w) : close(w; abort=true))
    end
    return dst
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
const INSPECT_LIMIT_FALLBACK = "inspection stopped because a resource limit was reached"
const INSPECT_OMITTED = "additional issues were omitted"

mutable struct InspectIssues
    data::Vector{String}
    len::Int
    fallback::String
    omitted::String
    fallbackused::Bool
    omittedused::Bool
end

function InspectIssues(budget::Budget)
    checkpoint = budgetcheckpoint(budget)
    charge = vectorbytes(String, INSPECT_ISSUE_CAPACITY) + shellbytes(InspectIssues)
    try
        reserve!(budget, charge)
        data = Vector{String}(undef, INSPECT_ISSUE_CAPACITY)
        allocated!(budget, vectorbytes(String, INSPECT_ISSUE_CAPACITY))
        issues = InspectIssues(data, 0, "", "", false, false)
        allocated!(budget, shellbytes(InspectIssues))
        issues.fallback = ownedstringcopy(INSPECT_LIMIT_FALLBACK, budget)
        issues.omitted = ownedstringcopy(INSPECT_OMITTED, budget)
        return issues
    catch
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

"Reserve the final issue slot for the bounded omission marker."
function inspectissueroom!(issues::InspectIssues)
    issues.len < INSPECT_ISSUE_CAPACITY - 1 && return true
    if issues.len == INSPECT_ISSUE_CAPACITY - 1
        issues.len += 1
        issues.data[issues.len] = issues.omitted
        issues.omittedused = true
    end
    return false
end

function pushinspectissue!(issues::InspectIssues, issue::String)
    issues.len += 1
    issues.data[issues.len] = issue
    return nothing
end

function staticinspectissue!(issues::InspectIssues, message::String, budget::Budget)
    inspectissueroom!(issues) || return nothing
    pushinspectissue!(issues, ownedstringcopy(message, budget))
    return nothing
end

"Build one owned, bounded diagnostic string without crediting it as encoded input."
function inspecttext(f::Function, budget::Budget, limits::Limits)
    checkpoint = budgetcheckpoint(budget)
    try
        writer = BoundedWriter(budget, limits.max_schema_bytes;
                               limit=:max_schema_bytes, credit=false)
        f(writer)
        text = boundedtake!(writer)
        release!(budget, bytesbytes(0))
        return text
    catch
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

function dynamicinspectissue!(f::Function, issues::InspectIssues, budget::Budget,
                              limits::Limits)
    inspectissueroom!(issues) || return nothing
    pushinspectissue!(issues, inspecttext(f, budget, limits))
    return nothing
end

function exceptioninspectissue!(issues::InspectIssues, error::AvroError,
                                budget::Budget, limits::Limits)
    inspectissueroom!(issues) || return nothing
    issue = try
        inspecttext(budget, limits) do writer
            showerror(writer, error)
        end
    catch
        issues.fallbackused = true
        issues.fallback
    end
    pushinspectissue!(issues, issue)
    return nothing
end

function finishinspectissues!(issues::InspectIssues, budget::Budget)
    issues.fallbackused || release!(budget, stringbytes(sizeof(issues.fallback)))
    issues.omittedused || release!(budget, stringbytes(sizeof(issues.omitted)))
    resize!(issues.data, issues.len)                    # fixed retained capacity stays charged
    data = issues.data
    release!(budget, shellbytes(InspectIssues))
    return data
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

A bounded, permissive diagnostic parse of a container source: never mutates it, tolerates repaired
schemas and legacy codec names, and stops at (and reports) the first structural problem.
"""
function inspect(src; limits::Limits=Limits())
    return withbudget(limits) do budget
        reportcharge = sizeof(InspectReport)
        reserve!(budget, reportcharge)                 # retained report shell has headroom on every exit
        issues = InspectIssues(budget)
        codecname = nothing
        schemaout = nothing
        blocks = 0
        datums = 0
        cbytes = 0
        codecstate = nothing
        source = src isa IOBuffer ? opensource(src, Val(:trim)) : opensource(src)
        try
            h = try
                readheader(source, limits, budget; legacy=:avrojl1, allow_invalid_names=true, allow_invalid_defaults=true)
            catch e
                e isa AvroError || rethrow()
                exceptioninspectissue!(issues, e, budget, limits)
                nothing
            end
            if h !== nothing
                codecname = h.codecname
                schemaout = h.schema
                h.codecname == "zstd" &&
                    staticinspectissue!(issues, "codec \"zstd\" is Avro.jl ≤ 1.1.2's name for zstandard; read with legacy=:avrojl1", budget)
                if !knowncodecname(h.codecname; legacy=true)
                    dynamicinspectissue!(issues, budget, limits) do writer
                        print(writer, "unknown codec ")
                        escapejson(writer, h.codecname)
                    end
                else
                    codecstate = try
                        readercodec(h.codecname, limits, :avrojl1)
                    catch e
                        e isa AvroError || rethrow()
                        exceptioninspectissue!(issues, e, budget, limits)
                        nothing
                    end
                end
                gi = graphinfo(h.schema)
                gi.repaired_names &&
                    staticinspectissue!(issues, "the schema contains invalid names; read with allow_invalid_names=true", budget)
                gi.repaired_defaults &&
                    staticinspectissue!(issues, "the schema contains invalid defaults; read with allow_invalid_defaults=true", budget)
                inspectdecimalwarnings!(issues, h.schema, budget, limits)
                inspectplan = inspectspan = nothing
                if codecstate !== nothing
                    try
                        inspectplan = readplan(h.schema; budget=budget)
                        inspectspan = spanplan(h.schema; budget=budget)
                    catch e
                        e isa AvroError || rethrow()
                        exceptioninspectissue!(issues, e, budget, limits)
                    end
                end
                while codecstate !== nothing && inspectplan !== nothing &&
                      inspectspan !== nothing && !sourceeof(source)
                    checkpoint = budgetcheckpoint(budget)
                    payload = decoded = nothing
                    try
                        work = BlockWork(budget.values, budget.input_bytes)
                        count = sourcevarint(source)
                        count >= 0 ||
                            throw(DataError("negative block count $count", position(source)))
                        count <= limits.max_block_count ||
                            throw(LimitError(:max_block_count, Int(count),
                                             limits.max_block_count,
                                             :max_block_count, :decode))
                        size = sourcevarint(source)
                        size >= 0 ||
                            throw(DataError("negative block size $size", position(source)))
                        size <= limits.max_block_bytes ||
                            throw(LimitError(:max_block_bytes, Int(size),
                                             limits.max_block_bytes,
                                             :max_block_bytes, :decode))
                        addblocks!(budget)
                        addrows!(budget, Int(count))
                        payload = sourcepayload(source, Int(size), budget)
                        for i in 1:16
                            sourceeof(source) &&
                                throw(DataError("truncated file", position(source)))
                            sourcebyte(source) == h.sync[i] ||
                                throw(DataError("sync marker mismatch after block $(blocks + 1)",
                                                position(source)))
                        end
                        addinput!(budget, varintlength(count) +
                                          varintlength(size) + 16)
                        cname, cstate = codecstate
                        if cname === :null && payload isa Vector{UInt8}
                            addinput!(budget, length(payload))
                            addmembers!(budget)
                            decoded = payload
                            payload = nothing
                        else
                            decoded = decompressblock(cname, cstate, payload,
                                                      limits, budget)
                            release!(budget, payloadcharge(source, Int(size)))
                            payload = nothing
                        end
                        n = Int(count)
                        checkblocklower!(budget, work, n,
                                         spanvalues(inspectspan))
                        decoder = setblockscope!(Decoder(decoded, budget;
                                                         validate=:strict), work)
                        for _ in 1:n
                            datumwork = begindatum!(decoder,
                                                    inspectspan::SpanPlan,
                                                    inspectplan::ReadPlan,
                                                    limits)
                            try
                                structuralskip(inspectplan::ReadPlan, decoder)
                            catch
                                abortdatum!(decoder, datumwork)
                                rethrow()
                            end
                            finishdatum!(decoder, datumwork)
                        end
                        if decoder.pos != length(decoded) + 1
                            if cname === :null
                                trailing = length(decoded) - decoder.pos + 1
                                dynamicinspectissue!(issues, budget, limits) do writer
                                    print(writer, "block ", blocks + 1, " has ",
                                          trailing, " trailing payload bytes after ", n,
                                          " datums (Avro.jl ≤ 1.1.2 null-codec padding; read with legacy=:avrojl1)")
                                end
                                removeinput!(budget, trailing)
                            else
                                throw(DataError("block $(blocks + 1) declares $n datums but they consume $(decoder.pos - 1) of $(length(decoded)) bytes",
                                                decoder.pos))
                            end
                        end
                        finishblockwork!(budget, work)
                        decodedcharge = bytesbytes(length(decoded))
                        decoded = nothing
                        release!(budget, decodedcharge)
                        blocks += 1
                        datums = checked_add(datums, n)
                        cbytes = checked_add(cbytes, Int(size))
                    catch e
                        e isa AvroError || rethrow()
                        payload = decoded = nothing
                        rollbackreservations!(budget, checkpoint)
                        exceptioninspectissue!(issues, e, budget, limits)
                        break
                    end
                end
            end
        catch e
            e isa AvroError || rethrow()
            exceptioninspectissue!(issues, e, budget, limits)
        finally
            closesource(source)
        end
        issuevector = finishinspectissues!(issues, budget)
        report = InspectReport(codecname, schemaout, blocks, datums, cbytes, issuevector)
        allocated!(budget, reportcharge)
        return report
    end
end

function inspectdecimalwarnings!(issues::InspectIssues, s::Schema, seen::Vector{Bool},
                                 budget::Budget, limits::Limits)
    issues.len == INSPECT_ISSUE_CAPACITY && return nothing
    index = Int(nodeid(s)) + 1
    seen[index] && return nothing
    seen[index] = true
    if s isa FixedSchema && s.logical isa DecimalLogical && s.size != 16
        dynamicinspectissue!(issues, budget, limits) do writer
            print(writer, "fixed decimal \"")
            writefullname(writer, s.name)
            print(writer, "\" has size ", s.size,
                  ": Avro.jl ≤ 1.1.2 wrote 16 bytes regardless, so 1.x files with this schema are misframed")
        end
    elseif s isa ArraySchema
        inspectdecimalwarnings!(issues, s.items, seen, budget, limits)
    elseif s isa MapSchema
        inspectdecimalwarnings!(issues, s.values, seen, budget, limits)
    elseif s isa UnionSchema
        for branch in s.branches
            inspectdecimalwarnings!(issues, branch, seen, budget, limits)
        end
    elseif s isa RecordSchema
        for field in s.fields
            inspectdecimalwarnings!(issues, field.schema, seen, budget, limits)
        end
    end
    return nothing
end

function inspectdecimalwarnings!(issues::InspectIssues, s::Schema, budget::Budget,
                                 limits::Limits)
    charge = vectorbytes(Bool, graphinfo(s).nodes)
    reserve!(budget, charge)
    seen = fill(false, graphinfo(s).nodes)
    allocated!(budget, charge)
    try
        inspectdecimalwarnings!(issues, s, seen, budget, limits)
    finally
        release!(budget, charge)
    end
    return nothing
end
