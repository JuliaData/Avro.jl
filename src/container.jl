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
end

const BlockSource = Union{BytesSource,StreamSource}

sourceeof(s::BytesSource) = s.pos > s.stop
sourceeof(s::StreamSource) = eof(s.io)

function sourcebyte(s::BytesSource)
    s.pos <= s.stop || throw(DataError("truncated file", s.pos))
    b = s.buf[s.pos]
    s.pos += 1
    return b
end

function sourcebyte(s::StreamSource)
    eof(s.io) && throw(DataError("truncated file", 0))
    return Base.read(s.io, UInt8)
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

Base.position(s::BytesSource) = s.pos
Base.position(s::StreamSource) = Int(position(s.io))

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
    readbytes!(s.io, out, n) == n || throw(DataError("truncated file", 0))
    return out
end

payloadcharge(::BytesSource, n::Int) = 0
payloadcharge(::StreamSource, n::Int) = bytesbytes(n)

closesource(::BytesSource) = nothing
closesource(s::StreamSource) = s.owned ? close(s.io) : nothing

opensource(src::Vector{UInt8}; mmap::Bool=true) = BytesSource(src, 1)
opensource(src::IOBuffer; mmap::Bool=true) = BytesSource(src.data, 1, src.size)
opensource(src::IO; mmap::Bool=true) = StreamSource(src, false)
function opensource(src::AbstractString; mmap::Bool=true)
    mmap || return StreamSource(open(src, "r"), true)
    return BytesSource(open(io -> Mmap.mmap(io, Vector{UInt8}), src, "r"), 1)
end

function opensource(src::IOBuffer, ::Val{:trim})
    return BytesSource(src.data, 1, src.size)
end

# ---- header -----------------------------------------------------------------------------------------

"The parsed container header: metadata (duplicates rejected), schema, codec name and sync marker."
function readheader(s::BlockSource, limits::Limits, budget::Budget; legacy, allow_invalid_names::Bool, allow_invalid_defaults::Bool)
    for m in MAGIC
        (sourceeof(s) || sourcebyte(s) != m) && throw(DataError("not an Avro object container file (bad magic)", position(s)))
    end
    keys = String[]
    vals = Vector{UInt8}[]
    total = 0
    while true
        count = sourcevarint(s)
        count == 0 && break
        size = -1
        if count < 0
            count == typemin(Int64) && throw(DataError("metadata block count typemin(Int64)", position(s)))
            count = -count
            size = sourcevarint(s)
            (0 <= size <= limits.max_metadata_bytes) || throw(LimitError(:max_metadata_bytes, Int(size), limits.max_metadata_bytes, :max_metadata_bytes, :decode))
        end
        blockstart = position(s)
        count <= limits.max_metadata_entries || throw(LimitError(:max_metadata_entries, Int(count), limits.max_metadata_entries, :max_metadata_entries, :decode))
        for _ in 1:count
            countvalues!(budget)
            length(keys) < limits.max_metadata_entries || throw(LimitError(:max_metadata_entries, length(keys) + 1, limits.max_metadata_entries, :max_metadata_entries, :decode))
            klen = sourcevarint(s)
            (0 <= klen <= limits.max_metadata_bytes) || throw(DataError("invalid metadata key length $klen", position(s)))
            kbytes = sourcepayload(s, Int(klen), budget)
            validutf8(kbytes, 1, length(kbytes)) || throw(DataError("metadata key is not valid UTF-8", position(s)))
            reserve!(budget, stringbytes(Int(klen)))               # the retained key, before its copy
            key = String(Vector{UInt8}(kbytes))
            release!(budget, payloadcharge(s, Int(klen)))          # a stream key buffer is transient
            vlen = sourcevarint(s)
            (0 <= vlen <= limits.max_metadata_bytes) || throw(DataError("invalid metadata value length $vlen", position(s)))
            total = checked_add(total, Int(klen) + Int(vlen))
            total <= limits.max_metadata_bytes || throw(LimitError(:max_metadata_bytes, total, limits.max_metadata_bytes, :max_metadata_bytes, :decode))
            vpayload = sourcepayload(s, Int(vlen), budget)
            value = if vpayload isa Vector{UInt8}
                vpayload                                            # a stream buffer is owned and already charged
            else
                reserve!(budget, bytesbytes(Int(vlen)))             # a byte-source view is copied, reserved first
                Vector{UInt8}(vpayload)
            end
            push!(keys, key)
            push!(vals, value)
        end
        if size >= 0
            consumed = position(s) - blockstart
            consumed == size || throw(DataError("metadata block declares $size bytes but its entries consume $consumed", position(s)))
        end
    end
    addinput!(budget, total)
    metadata = buildmap(Vector{UInt8}, keys, vals, budget; duplicateposition=position(s))
    sync = ntuple(_ -> sourcebyte(s), 16)
    schemabytes = get(metadata, "avro.schema", nothing)
    schemabytes === nothing && throw(DataError("the container has no avro.schema", position(s)))
    schema = parseschema(schemabytes; allow_invalid_names=allow_invalid_names, allow_invalid_defaults=allow_invalid_defaults, limits=limits,
                         legacy_fixed_names=legacy === :avrojl1, budget=budget)
    codecbytes = get(metadata, "avro.codec", nothing)
    codecname = "null"
    if codecbytes !== nothing
        validutf8(codecbytes, 1, length(codecbytes)) || throw(DataError("avro.codec is not valid UTF-8", position(s)))
        codecname = String(copy(codecbytes))
    end
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
    const source::BlockSource
    const schema::Schema
    const plan::ReadPlan
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

function Reader(src; limits::Limits=Limits(), legacy::Union{Nothing,Symbol}=nothing, decimal_byteorder::Symbol=:big,
                allow_invalid_names::Bool=false, allow_invalid_defaults::Bool=false, validate::Symbol=:strict, mmap::Bool=true)
    validate in (:strict, :fast) || throw(ArgumentError("validate must be :strict or :fast"))
    decimal_byteorder in (:big, :little) || throw(ArgumentError("decimal_byteorder must be :big or :little"))
    legacy in (nothing, :avrojl1) || throw(ArgumentError("legacy must be nothing or :avrojl1"))
    source = src isa IOBuffer ? opensource(src, Val(:trim)) : opensource(src; mmap=mmap)
    budget = Budget(limits)
    r = try
        h = readheader(source, limits, budget; legacy=legacy, allow_invalid_names=allow_invalid_names, allow_invalid_defaults=allow_invalid_defaults)
        cname, codec = readercodec(h.codecname, limits, legacy)
        plan = withplanbudget(budget) do
            p = readplan(h.schema; budget=budget)
            decimal_byteorder === :little ? littledecimals(p) : p
        end
        Reader(source, h.schema, plan, cname, codec, h.metadata, h.sync, limits, budget, validate, legacy, 0, false, false)
    catch
        closesource(source)
        close!(budget)
        rethrow()
    end
    finalizer(r) do x
        x.closed || (closesource(x.source); close!(x.budget); x.closed = true)
    end
    return r
end

withplanbudget(f, budget) = f()

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
    r.closed = true
    closesource(r.source)
    close!(r.budget)
    return nothing
end

checkopen(r::Reader) = r.closed ? throw(ArgumentError("the reader is closed")) : nothing

"""
    Avro.metadata(r) -> Avro.Map{Vector{UInt8}}; Avro.codec(r) -> Symbol; Avro.sync(r) -> NTuple{16,UInt8}
    Avro.writerschema(r) -> Schema; Avro.schema(r) -> Schema
"""
metadata(r::Reader) = r.metadata
codec(r::Reader) = r.codecname
sync(r::Reader) = r.sync
writerschema(r::Reader) = r.schema
schema(r::Reader) = r.schema

"Rewrite a plan tree so decimals decode little-endian (`decimal_byteorder=:little`, plan §4.9)."
function littledecimals(p::ReadPlan, memo::IdDict{Any,Any}=IdDict{Any,Any}())
    haskey(memo, p) && return memo[p]::ReadPlan
    if p isa DecimalPlan
        out = DecimalPlan(p.fixedsize, p.precision, p.scale, p.wide, true)
        memo[p] = out
        return out
    elseif p isa ArrayPlan
        out = ArrayPlan(littledecimals(p.items, memo), p.eltype, p.minsize)
        memo[p] = out
        return out
    elseif p isa MapPlan
        out = MapPlan(littledecimals(p.values, memo), p.eltype, p.minsize)
        memo[p] = out
        return out
    elseif p isa UnionPlan
        out = UnionPlan(ReadPlan[], p.nullable)
        memo[p] = out
        for b in p.branches
            push!(out.branches, littledecimals(b, memo))
        end
        return out
    elseif p isa RecordPlan
        out = RecordPlan(p.schema, ReadPlan[], copy(p.boxes))
        memo[p] = out
        for f in p.fields
            push!(out.fields, littledecimals(f, memo))
        end
        return out
    end
    if p isa ResolvedRecordPlan
        out = ResolvedRecordPlan(p.schema, Pair{Int,ReadPlan}[], copy(p.defaults), copy(p.boxes))
        memo[p] = out
        for (slot, plan) in p.steps
            push!(out.steps, slot => littledecimals(plan, memo))
        end
        return out
    elseif p isa UnionResolvePlan
        out = UnionResolvePlan(ReadPlan[], p.readerindex, p.nullable, p.readerunion)
        memo[p] = out
        for b in p.branches
            push!(out.branches, littledecimals(b, memo))
        end
        return out
    elseif p isa WrapPlan
        out = WrapPlan(littledecimals(p.inner, memo), p.readerindex, p.nullable)
        memo[p] = out
        return out
    elseif p isa PromotePlan
        out = PromotePlan(p.writer, littledecimals(p.reader, memo))
        memo[p] = out
        return out
    end
    memo[p] = p
    return p
end

"Read the next block into owned decompressed bytes; `nothing` at a clean end of the file."
function nextblock!(r::Reader; walk::Bool=true)
    checkopen(r)
    sourceeof(r.source) && return nothing
    checkpoint = r.budget.reserved
    try
        count = sourcevarint(r.source)
        (0 <= count <= r.limits.max_block_count) || (count < 0 ? throw(DataError("negative block count $count", position(r.source))) :
                                                     throw(LimitError(:max_block_count, Int(count), r.limits.max_block_count, :max_block_count, :decode)))
        size = sourcevarint(r.source)
        (0 <= size <= r.limits.max_block_bytes) || (size < 0 ? throw(DataError("negative block size $size", position(r.source))) :
                                                    throw(LimitError(:max_block_bytes, Int(size), r.limits.max_block_bytes, :max_block_bytes, :decode)))
        r.blockindex += 1
        r.blockindex <= r.limits.max_blocks || throw(LimitError(:max_blocks, r.blockindex, r.limits.max_blocks, :max_blocks, :decode))
        addblocks!(r.budget)
        addrows!(r.budget, Int(count))
        payload = sourcepayload(r.source, Int(size), r.budget)
        for i in 1:16
            (sourceeof(r.source) ? throw(DataError("truncated file", position(r.source))) : sourcebyte(r.source)) == r.sync[i] ||
                throw(DataError("sync marker mismatch after block $(r.blockindex)", position(r.source)))
        end
        addinput!(r.budget, varintlength(count) + varintlength(size) + 16)
        bytes = if r.codecname === :null && payload isa Vector{UInt8}
            addinput!(r.budget, length(payload))
            addmembers!(r.budget)
            payload                                       # a streamed null-codec payload is already owned
        else
            out = decompressblock(r.codecname, r.codec, payload, r.limits, r.budget)
            release!(r.budget, payloadcharge(r.source, Int(size)))
            out
        end
        n = Int(count)
        if r.validate === :strict && (walk || r.legacy === :avrojl1)
            d = Decoder(bytes, r.budget)
            for _ in 1:n
                skip(r.plan, d)
            end
            if d.pos != length(bytes) + 1
                if r.legacy === :avrojl1 && r.codecname === :null
                    r.warned || (@warn "accepting trailing bytes after $(n) datums in a null-codec block (legacy=:avrojl1; Avro.jl ≤ 1.1.2 sizing cushion)" source = 1; r.warned = true)
                    resize!(bytes, d.pos - 1)
                else
                    throw(DataError("block $(r.blockindex) declares $n datums but they consume $(d.pos - 1) of $(length(bytes)) bytes", d.pos))
                end
            end
        end
        release!(r.budget, bytesbytes(length(bytes)))     # ownership transfers to the caller at yield
        return (n, bytes)
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
eachblock(r::Reader) = EachBlock(r)
Base.IteratorSize(::Type{EachBlock}) = Base.SizeUnknown()
Base.eltype(::Type{EachBlock}) = Tuple{Int,Vector{UInt8}}
Base.iterate(it::EachBlock, ::Nothing=nothing) = (b = nextblock!(it.reader); b === nothing ? nothing : (b, nothing))

mutable struct EachDatum
    const reader::Reader
    bytes::Vector{UInt8}
    decoder::Union{Nothing,Decoder{Vector{UInt8}}}
    remaining::Int
    lastcharge::Int
    blockout::Int        # the block'''s cumulative decoded output (max_block_output_bytes, charged incrementally)
end

"""
    Avro.eachdatum(r::Reader)

Iterate the file's datums as the schema's generic values (any root schema). Each yielded value is the
caller's at yield; its charge is released at the next iteration step.
"""
eachdatum(r::Reader) = EachDatum(r, UInt8[], nothing, 0, 0, 0)
Base.IteratorSize(::Type{EachDatum}) = Base.SizeUnknown()

function Base.iterate(it::EachDatum, ::Nothing=nothing)
    b = it.reader.budget
    release!(b, it.lastcharge)
    it.lastcharge = 0
    while it.remaining == 0
        blk = nextblock!(it.reader; walk=false)
        blk === nothing && return nothing
        it.remaining = blk[1]
        it.bytes = blk[2]
        it.blockout = 0
        reserve!(b, bytesbytes(length(it.bytes)))     # the block is resident while its datums decode
        it.decoder = Decoder(it.bytes, b; validate=it.reader.validate)
        it.remaining == 0 && release!(b, bytesbytes(length(it.bytes)))
    end
    it.remaining -= 1
    before = b.reserved
    v = decode(it.reader.plan, it.decoder)
    it.lastcharge = max(b.reserved - before, 0)
    it.blockout = checked_add(it.blockout, it.lastcharge + STORAGE[].slot)
    it.blockout <= b.limits.max_block_output_bytes ||
        throw(LimitError(:max_block_output_bytes, it.blockout, b.limits.max_block_output_bytes, :max_block_output_bytes, :decode))
    if it.remaining == 0
        d = it.decoder
        if d.pos != length(it.bytes) + 1 && it.reader.validate === :strict
            throw(DataError("block datums did not consume the block exactly", d.pos))
        end
        release!(b, bytesbytes(length(it.bytes)))
    end
    return (v, nothing)
end

# ---- the reader-side output estimate of encoded values (plan §4.9, one consumer-independent formula) --

"(bytes, values) a reader would charge for `x` under `p`: category-(b) payload plus slot bytes."
function estimatevalue(p::WritePlan, x)::Tuple{Int,Int}
    p isa WNull && return (0, 1)
    p isa Union{WBool,WInt,WDate,WTimeMillis} && return (4, 1)
    p isa Union{WLong,WTimeMicros,WTimestamp,WLocalTimestamp,WDouble} && return (8, 1)
    p isa WFloat && return (4, 1)
    p isa WDuration && return (12, 1)
    p isa WUUIDFixed && return (16, 1)
    p isa WString && return (STORAGE[].slot + stringbytes(valuesizeof(x)), 1)
    p isa WUUIDString && return (STORAGE[].slot + 16, 1)
    p isa WBytes && return (STORAGE[].slot + bytesbytes(valuelength(x)), 1)
    p isa WFixed && return (STORAGE[].slot + fixedbytes(p.schema.size), 1)
    p isa WEnum && return (2 * enumvaluebytes(), 1)
    p isa WDecimal && return (p.precision <= 38 ? boxbytes(Decimal) : STORAGE[].slot + widedecimalbytes(cld(p.precision, 2)), 1)
    if p isa WUnion
        inner = x isa UnionValue ? x.value : x
        i = try
            selectbranch(p, x)
        catch
            return (unionvaluebytes(), 1)
        end
        eb, ev = estimatevalue(p.branches[i], inner)
        return (checked_add(unionvaluebytes(), eb), 1 + ev)
    end
    if p isa WArray
        b, v = STORAGE[].vector, 1
        items = try
            arrayitems(x)
        catch
            return (b, v)
        end
        for el in items
            eb, ev = estimatevalue(p.items, el)
            b = checked_add(b, eb)
            v += ev
        end
        return (b, v)
    end
    if p isa WMap
        b, v = STORAGE[].map + 3 * STORAGE[].vector, 1
        prs = try
            mappairs(x)
        catch
            return (b, v)
        end
        for (k, el) in prs
            eb, ev = estimatevalue(p.values, el)
            b = checked_add(b, checked_add(stringbytes(valuesizeof(k)) + 12, eb))
            v += ev
        end
        return (b, v)
    end
    if p isa WRecord
        b, v = recordbytes(length(p.fields)), 1
        for (i, f) in enumerate(p.fields)
            fv = try
                recordfieldvalue(p, x, i)
            catch
                continue
            end
            eb, ev = estimatevalue(f, fv)
            b = checked_add(b, eb)
            v += ev
        end
        return (b, v)
    end
    return (0, 1)
end

valuesizeof(x) = x isa AbstractString ? sizeof(x) : x isa Symbol ? sizeof(String(x)) : 0
valuelength(x) = x isa AbstractVector{UInt8} ? length(x) : 0

function recordfieldvalue(p::WRecord, x, i::Int)
    x isa Record && return getfield(x, :values)[i]
    x isa TableRow && return rowfield(x.row, p.schema.fields[i].name)
    name = p.schema.fields[i].name
    x isa AbstractDict && return dictfield(x, name)
    x isa Tables.AbstractRow && return rowfield(x, name)
    positions = fieldpositions(p, typeof(x))
    j = positions[i]
    j == 0 && throw(ArgumentError("no field"))
    return getfield(x, j)
end

# ---- the writer's reader preflight (plan §4.4) -------------------------------------------------------

"The reader-retained charge of the block table: one entry per block (offset, size, count, cumulative rows)."
blocktablecharge(nblocks::Int) = checked_add(STORAGE[].vector, checked_mul(32, nblocks))

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
    chunkbytes::Int      # chunk shells and capacities of the flushed blocks
    payload::Int         # referenced payload of the flushed blocks, counted once
    rows::Int
    nblocks::Int
    pendingpayload::Int  # referenced payload of the pending block
end

function TablePreflight(readerschema::RecordSchema, p::WRecord)
    cols = Type[juliatype(f.schema) for f in readerschema.fields]
    slack = Int[cellslack(f) for f in p.fields]
    return TablePreflight(cols, slack, 0, 0, 0, 0, 0)
end

"The §4.9 estimate of one record-root datum plus its column-consumer payload beyond the chunk slots."
function estimaterootrecord(p::WRecord, pf::TablePreflight, x)
    b, v = recordbytes(length(p.fields)), 1
    payload = 0
    for (i, f) in enumerate(p.fields)
        fv = try
            recordfieldvalue(p, x, i)
        catch
            continue
        end
        eb, ev = estimatevalue(f, fv)
        b = checked_add(b, eb)
        v += ev
        payload = checked_add(payload, max(eb - pf.slack[i], 0))
    end
    return (b, v, payload)
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
    const preflightbase::Int
    const preflight::Union{Nothing,TablePreflight}
    fasttype::Any                    # single-entry aligned-NamedTuple cache (Phase 4d)
    fastplans::Any
    pendingcount::Int
    pendingbytes::Int
    pendingvalues::Int
    closed::Bool
    poison::Union{Nothing,Exception}
end

function Writer(dst::Union{AbstractString,IO}, schema::Schema; codec::Symbol=:null, level=nothing,
                metadata=Dict{String,Vector{UInt8}}(), sync=nothing, block_bytes::Integer=64 * 1024,
                atomic::Bool=true, fsync::Bool=false, allow_invalid_names::Bool=false,
                allow_invalid_defaults::Bool=false, limits::Limits=Limits())
    0 < block_bytes <= limits.max_block_bytes || throw(ArgumentError("block_bytes must be in 1:$(limits.max_block_bytes), got $block_bytes"))
    gi = graphinfo(schema)
    gi.repaired_names && !allow_invalid_names && throw(ArgumentError("the schema contains invalid names; pass allow_invalid_names=true to write it"))
    gi.repaired_defaults && !allow_invalid_defaults && throw(ArgumentError("the schema contains invalid defaults; pass allow_invalid_defaults=true to write it"))
    syncmarker = if sync === nothing
        ntuple(_ -> rand(RandomDevice(), UInt8), 16)
    else
        (sync isa AbstractVector{UInt8} && length(sync) == 16) || throw(ArgumentError("sync must be exactly 16 bytes"))
        ntuple(i -> sync[i], 16)
    end
    metadata isa AbstractDict{<:AbstractString,<:AbstractVector{UInt8}} || throw(ArgumentError("metadata must map strings to byte vectors"))
    budget = Budget(limits; direction=:encode)                    # the budget exists before any header allocation (R04)
    wcodec = writercodec(codec, level, limits)
    w = try
        reserve!(budget, wcodec.workspace)
        jw = BoundedWriter(budget, limits.max_schema_bytes)       # the schema JSON is produced charged and bounded
        printschema(jw, schema, "", FrozenDict{String,Bool}(), false, 0)
        schemajson = String(take!(jw.io))
        entries = Tuple{String,Vector{UInt8}}[]
        reserve!(budget, stringbytes(11) + bytesbytes(sizeof(schemajson)))
        push!(entries, ("avro.schema", Vector{UInt8}(codeunits(schemajson))))
        reserve!(budget, stringbytes(10) + bytesbytes(sizeof(String(codec))))
        push!(entries, ("avro.codec", Vector{UInt8}(codeunits(String(codec)))))
        for (k, v) in metadata
            startswith(k, "avro.") && throw(ArgumentError("metadata keys in the avro.* namespace are reserved (got \"$k\"); avro.schema and avro.codec come from the constructor"))
            isstrictutf8(k) || throw(ArgumentError("metadata keys must be valid UTF-8"))
            reserve!(budget, stringbytes(sizeof(k)) + bytesbytes(length(v)))   # each retained copy, before it is made
            push!(entries, (String(k), Vector{UInt8}(v)))
        end
        length(entries) <= limits.max_metadata_entries || throw(LimitError(:max_metadata_entries, length(entries), limits.max_metadata_entries, :max_metadata_entries, :encode))
        total = sum(e -> sizeof(e[1]) + length(e[2]), entries; init=0)
        total <= limits.max_metadata_bytes || throw(LimitError(:max_metadata_bytes, total, limits.max_metadata_bytes, :max_metadata_bytes, :encode))
        plan = writeplan(schema; budget=budget)
        # The reader's construction retention, preflighted under the writer's budget (plan §4.4): the
        # parsed schema graph a reader builds from the same JSON, the materialised metadata of a stream
        # reader (key and value buffers plus the retained entries and map), and the generic read plan.
        base0 = budget.reserved
        pfschema = parseschema(schemajson; allow_invalid_names=allow_invalid_names, allow_invalid_defaults=allow_invalid_defaults,
                               limits=limits, budget=budget)
        pfkeys = String[]
        pfvals = Vector{UInt8}[]
        for (k, v) in entries
            reserve!(budget, checked_add(stringbytes(sizeof(k)), bytesbytes(length(v))))  # the retained metadata entry (byte and stream readers now charge identically)
            push!(pfkeys, k)
            push!(pfvals, v)
        end
        buildmap(Vector{UInt8}, pfkeys, pfvals, budget)
        withplanbudget(budget) do
            readplan(pfschema; budget=budget)
        end
        preflightbase = budget.reserved - base0
        preflight = pfschema isa RecordSchema ? TablePreflight(pfschema, plan::WRecord) : nothing
        path = dst isa AbstractString ? String(dst) : nothing
        sink, temppath, ownsink = if path === nothing
            (dst, nothing, false)
        elseif atomic
            t, tio = mktemp(dirname(abspath(path)))
            (tio, t, true)
        else
            (open(path, "w"), nothing, true)
        end
        encoder = Encoder(budget)
        w = Writer(sink, path, atomic ? temppath : nothing, schema, plan, wcodec, syncmarker, limits, budget,
                   Int(block_bytes), atomic, fsync, ownsink, encoder, preflightbase, preflight, nothing, nothing, 0, 0, 0, false, nothing)
        try
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
        catch e
            poison!(w, e)
            abortcleanup(w)
            rethrow()
        end
        w
    catch
        close!(budget)
        rethrow()
    end
    finalizer(w) do x
        x.closed || (x.closed = true; abortcleanup(x); close!(x.budget))
    end
    return w
end

function Writer(f::Function, dst, schema::Schema; kw...)
    w = Writer(dst, schema; kw...)
    ok = false
    try
        r = f(w)
        ok = true
        return r
    finally
        ok ? close(w) : close(w; abort=true)
    end
end

poison!(w::Writer, e::Exception) = (w.poison === nothing && (w.poison = e); nothing)

function checkwritable(w::Writer)
    w.closed && throw(WriterClosedError(w.poison))
    w.poison === nothing || throw(WriterClosedError(w.poison))
    return nothing
end

function abortcleanup(w::Writer)
    try
        w.ownsink && close(w.sink)
    catch
    end
    w.temppath !== nothing && rm(w.temppath; force=true)
    return nothing
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
    nextrows = checked_add(w.budget.rows, 1)
    nextrows <= w.limits.max_rows || throw(limiterror(w.budget, :max_rows, nextrows, w.limits.max_rows))
    pf = w.preflight
    fp = nothing
    if datum isa NamedTuple
        if w.fasttype === typeof(datum)
            fp = w.fastplans
        else
            fp = alignedplans(w.plan, typeof(datum))
            w.fasttype = typeof(datum)
            w.fastplans = fp
        end
    end
    if fp !== nothing
        eb, ev, pp = estimatealigned(fp, datum, pf === nothing ? nothing : pf.slack)
    elseif pf === nothing
        eb, ev = estimatevalue(w.plan, datum)
        pp = 0
    else
        eb, ev, pp = estimaterootrecord(w.plan::WRecord, pf, datum)
    end
    eb > w.limits.max_block_output_bytes &&
        throw(LimitError(:max_block_output_bytes, eb, w.limits.max_block_output_bytes, :max_block_output_bytes, :encode))
    if w.pendingcount > 0 && (checked_add(w.pendingbytes, eb) > w.limits.max_block_output_bytes ||
                              w.pendingcount + 1 > w.limits.max_block_count ||
                              w.pendingvalues + ev > checked_add(checked_mul(w.limits.max_values_per_byte, max(w.encoder.pos, 64)), w.limits.work_allowance))
        flushblock!(w)
    end
    nextcount = checked_add(w.pendingcount, 1)
    nextcount <= w.limits.max_block_count ||
        throw(limiterror(w.budget, :max_block_count, nextcount, w.limits.max_block_count))
    start = w.encoder.pos
    try
        fp === nothing ? encodedatum!(w.plan, w.encoder, datum) : encodealigned!(fp, w.encoder, datum)
        addrows!(w.budget, 1)
    catch
        w.encoder.pos = start                          # a rejected datum leaves the pending block intact
        rethrow()
    end
    w.pendingcount += 1
    w.pendingbytes = checked_add(w.pendingbytes, eb)
    w.pendingvalues += ev
    pf === nothing || (pf.pendingpayload = checked_add(pf.pendingpayload, pp))
    w.encoder.pos >= w.blockbytes && flushblock!(w)
    return w
end

function Base.write(w::Writer, datums)
    for d in datums
        push!(w, d)
    end
    return w
end

function flushblock!(w::Writer)
    w.pendingcount == 0 && return nothing
    try
        nextblocks = checked_add(w.budget.blocks, 1)
        nextblocks <= w.limits.max_blocks ||
            throw(limiterror(w.budget, :max_blocks, nextblocks, w.limits.max_blocks))
        pf = w.preflight
        chunk = payload = rows = 0
        if pf !== nothing
            # The streamed Table consumer's peak with this block included (plan §4.4): chunk shells and
            # capacities, referenced payload once, both sets of reference slots (finals fully reserved
            # before the chunks release), the block table, and the reader's construction retention.
            n = w.pendingcount
            chunk = pf.chunkbytes
            finals = 0
            rows = pf.rows + n
            for E in pf.cols
                chunk = checked_add(chunk, vectorbytes(E, n))
                finals = checked_add(finals, vectorbytes(E, rows))
            end
            payload = checked_add(pf.payload, pf.pendingpayload)
            projected = checked_add(checked_add(w.preflightbase, blocktablecharge(pf.nblocks + 1)),
                                    checked_add(checked_add(chunk, payload), finals))
            projected <= w.budget.ceiling ||
                throw(LimitError(:max_total_bytes, projected, w.budget.ceiling, :max_total_bytes, :encode))
        end
        reserve!(w.budget, bytesbytes(w.encoder.pos))              # the pending-block copy, before take! (R04)
        blockbytes = take!(w.encoder)
        cbound = compressbound(w.wcodec, length(blockbytes))
        reserve!(w.budget, cbound)                                  # the compressor's output bound, before it allocates
        compressed = compressblock(w.wcodec, blockbytes)
        length(compressed) <= w.limits.max_block_bytes ||
            throw(LimitError(:max_block_bytes, length(compressed), w.limits.max_block_bytes, :max_block_bytes, :encode))
        verifyframe(w.wcodec, compressed, w.limits)
        peak = readerblockpeak(length(compressed), length(blockbytes), w.pendingbytes,
                               w.wcodec.name === :null ? 0 : w.limits.max_codec_memory, w.wcodec.name === :null)
        reserve!(w.budget, peak)                       # one block's transient reader peak fits the ceiling
        release!(w.budget, peak)
        if pf !== nothing
            pf.chunkbytes = chunk
            pf.payload = payload
            pf.rows = rows
            pf.nblocks += 1
            pf.pendingpayload = 0
        end
        writevarint(w.sink, w.pendingcount)
        writevarint(w.sink, length(compressed))
        Base.write(w.sink, compressed)
        for b in w.syncmarker
            Base.write(w.sink, b)
        end
        addinput!(w.budget, varintlength(w.pendingcount) + varintlength(length(compressed)) + 16)   # the reader's framing denominator (R08)
        release!(w.budget, checked_add(bytesbytes(length(blockbytes)), cbound))                     # the flush transients
        addblocks!(w.budget)
    catch e
        poison!(w, e)
        rethrow()
    end
    w.pendingcount = 0
    w.pendingbytes = 0
    w.pendingvalues = 0
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
        abortcleanup(w)
        close!(w.budget)
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
        abortcleanup(w)
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
    issues = String[]
    codecname = nothing
    schemaout = nothing
    blocks = 0
    datums = 0
    cbytes = 0
    withbudget(limits) do budget
        source = src isa IOBuffer ? opensource(src, Val(:trim)) : opensource(src)
        try
            h = try
                readheader(source, limits, budget; legacy=:avrojl1, allow_invalid_names=true, allow_invalid_defaults=true)
            catch e
                push!(issues, sprint(showerror, e))
                return nothing
            end
            codecname = h.codecname
            schemaout = h.schema
            h.codecname == "zstd" && push!(issues, "codec \"zstd\" is Avro.jl ≤ 1.1.2's name for zstandard; read with legacy=:avrojl1")
            Symbol(h.codecname) in (:null, :deflate, :snappy, :zstandard, :bzip2, :xz, :zstd) ||
                push!(issues, "unknown codec \"$(escapename(h.codecname))\"")
            gi = graphinfo(h.schema)
            gi.repaired_names && push!(issues, "the schema contains invalid names; read with allow_invalid_names=true")
            gi.repaired_defaults && push!(issues, "the schema contains invalid defaults; read with allow_invalid_defaults=true")
            for issue in decimalsizewarnings(h.schema)
                push!(issues, issue)
            end
            while !sourceeof(source)
                count = sourcevarint(source)
                if !(0 <= count <= limits.max_block_count)
                    push!(issues, "block $(blocks + 1) declares $count datums")
                    break
                end
                size = sourcevarint(source)
                if !(0 <= size <= limits.max_block_bytes)
                    push!(issues, "block $(blocks + 1) declares $size payload bytes")
                    break
                end
                payload = sourcepayload(source, Int(size), budget)
                release!(budget, payloadcharge(source, Int(size)))
                ok = true
                for i in 1:16
                    if sourceeof(source) || sourcebyte(source) != h.sync[i]
                        push!(issues, "sync marker mismatch after block $(blocks + 1)")
                        ok = false
                        break
                    end
                end
                blocks += 1
                datums += Int(count)
                cbytes += Int(size)
                ok || break
            end
        catch e
            push!(issues, sprint(showerror, e))
        finally
            closesource(source)
        end
        return nothing
    end
    return InspectReport(codecname, schemaout, blocks, datums, cbytes, issues)
end

function decimalsizewarnings(s::Schema, seen::Vector{Int32}=Int32[], out::Vector{String}=String[])
    if s isa FixedSchema && s.logical isa DecimalLogical && s.size != 16
        push!(out, "fixed decimal \"$(fullname(s))\" has size $(s.size): Avro.jl ≤ 1.1.2 wrote 16 bytes regardless, so 1.x files with this schema are misframed")
    elseif s isa ArraySchema
        decimalsizewarnings(s.items, seen, out)
    elseif s isa MapSchema
        decimalsizewarnings(s.values, seen, out)
    elseif s isa UnionSchema
        foreach(b -> decimalsizewarnings(b, seen, out), s.branches)
    elseif s isa RecordSchema
        nodeid(s) in seen && return out
        push!(seen, nodeid(s))
        foreach(f -> decimalsizewarnings(f.schema, seen, out), s.fields)
    end
    return out
end
