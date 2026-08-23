# Codecs (plan §4.9): block compression and strictly bounded decompression. `max_codec_memory` has
# exactly one meaning — the cap on the complete decoder memory requirement of one codec member as the
# codec library reports it — enforced on every member read and on every frame written. Multi-member
# payloads (zstandard frames incl. skippable ones; xz/bzip2 streams via the extensions) are decoded to
# exact exhaustion; deflate rejects bytes after its final block; snappy carries the big-endian CRC32 of
# the uncompressed data.

using CodecZlib: DeflateCompressor, DeflateDecompressor
using CodecZstd: ZstdCompressor, ZstdDecompressor
import Snappy
import TranscodingStreams
import Zstd_jll

const BUILTIN_CODECS = (:null, :deflate, :snappy, :zstandard)
const EXTENSION_PACKAGES = Dict{Symbol,String}(:bzip2 => "CodecBzip2", :xz => "CodecXz")
const EXTENSION_CODECS = Dict{Symbol,Any}()   # name => (; reader=limits -> state, writer=(level, limits) -> WriterCodec)

"Register an extension codec's factories (called from the extension module's `__init__`)."
registercodec!(name::Symbol, factories) = (EXTENSION_CODECS[name] = factories; nothing)

"""
    Avro.codecs() -> Vector{Symbol}

The container codecs this process can read and write (`:bzip2` and `:xz` appear once their extension
packages are loaded).
"""
codecs() = Symbol[BUILTIN_CODECS..., sort!(collect(keys(EXTENSION_CODECS)))...]

struct NullCodec end
struct DeflateReader end
struct SnappyCodec end
struct ZstdReader
    windowlogmax::Int32
end

"The reader state for the codec named in `avro.codec` (`legacy=:avrojl1` reads `zstd` as zstandard)."
function readercodec(name::AbstractString, limits::Limits, legacy::Union{Nothing,Symbol})
    sym = Symbol(name)
    legacy === :avrojl1 && sym === :zstd && (sym = :zstandard)
    sym === :null && return (sym, NullCodec())
    sym === :deflate && return (sym, DeflateReader())
    sym === :snappy && return (sym, SnappyCodec())
    sym === :zstandard && return (sym, ZstdReader(zstdwindowlogmax(limits.max_codec_memory)))
    haskey(EXTENSION_CODECS, sym) && return (sym, EXTENSION_CODECS[sym].reader(limits))
    throw(UnsupportedCodecError(String(name), get(EXTENSION_PACKAGES, sym, nothing)))
end

const DEFLATE_DECODER_BYTES = 1 << 16       # zlib inflate state incl. the 32 KiB window
const DEFLATE_ENCODER_BYTES = 320 * 1024    # zlib deflate state at any level

# ---- zstandard library calls (Zstd_jll; the sizing functions are required symbols, plan §11) ---------

zstd_iserror(code::Csize_t) = ccall((:ZSTD_isError, Zstd_jll.libzstd), Cuint, (Csize_t,), code) != 0
zstd_dstreamsize(windowsize::Integer) = Int(ccall((:ZSTD_estimateDStreamSize, Zstd_jll.libzstd), Csize_t, (Csize_t,), windowsize))
zstd_cstreamsize(level::Integer) = Int(ccall((:ZSTD_estimateCStreamSize, Zstd_jll.libzstd), Csize_t, (Cint,), level))

struct ZstdCParams
    windowLog::Cuint
    chainLog::Cuint
    hashLog::Cuint
    searchLog::Cuint
    minMatch::Cuint
    targetLength::Cuint
    strategy::Cint
end

zstd_cparams(level::Integer) = ccall((:ZSTD_getCParams, Zstd_jll.libzstd), ZstdCParams, (Cint, Culonglong, Csize_t), level, 0, 0)

function zstd_framesize(buf::AbstractVector{UInt8}, from::Int, len::Int)
    code = GC.@preserve buf ccall((:ZSTD_findFrameCompressedSize, Zstd_jll.libzstd), Csize_t, (Ptr{Cvoid}, Csize_t), pointer(buf, from), len)
    zstd_iserror(code) && return nothing
    return Int(code)
end

function zstd_frameestimate(buf::AbstractVector{UInt8}, from::Int, len::Int)
    code = GC.@preserve buf ccall((:ZSTD_estimateDStreamSize_fromFrame, Zstd_jll.libzstd), Csize_t, (Ptr{Cvoid}, Csize_t), pointer(buf, from), len)
    zstd_iserror(code) && return nothing
    return Int(code)
end

zstd_skippable(buf::AbstractVector{UInt8}, from::Int, len::Int) =
    GC.@preserve buf ccall((:ZSTD_isSkippableFrame, Zstd_jll.libzstd), Cuint, (Ptr{Cvoid}, Csize_t), pointer(buf, from), len) != 0

"The largest windowLog in 10…31 whose reported decoder estimate fits `cap` (the 16 MiB floor admits ≥ 23)."
function zstdwindowlogmax(cap::Int)
    best = 0
    for L in 10:31
        zstd_dstreamsize(Int64(1) << L) <= cap && (best = L)
    end
    return Int32(best)
end

# ---- the member loop over a TranscodingStreams codec -------------------------------------------------

function growbuffer!(budget::Budget, buf::Vector{UInt8}, newcap::Int, len::Int)
    reserve!(budget, bytesbytes(newcap))
    nb = Vector{UInt8}(undef, newcap)
    copyto!(nb, 1, buf, 1, len)
    release!(budget, bytesbytes(length(buf)))
    return nb
end

function shrinkexact(budget::Budget, buf::Vector{UInt8}, len::Int)
    len == length(buf) && return buf
    reserve!(budget, bytesbytes(len))
    out = Vector{UInt8}(undef, len)
    copyto!(out, 1, buf, 1, len)
    release!(budget, bytesbytes(length(buf)))
    return out
end

codecmessage(err::TranscodingStreams.Error) = TranscodingStreams.haserror(err) ? sprint(showerror, err.error) : "codec failure"

"""
    transcodemember!(name, codec, input, from, to, out, outlen, maxout, budget) -> (consumed, out, outlen)

Decompress one member with a TranscodingStreams codec, appending into `out` (grown by exact replacement
under `budget`, capped at `maxout` → `LimitError(:max_block_bytes)`); a member that consumes no bytes
and produces none with input exhausted is truncated (`CodecError`).
"""
function transcodemember!(name::Symbol, codec::TranscodingStreams.Codec, input::AbstractVector{UInt8}, from::Int, to::Int,
                          out::Vector{UInt8}, outlen::Int, maxout::Int, budget::Budget)
    err = TranscodingStreams.Error()
    TranscodingStreams.initialize(codec)
    inpos = from
    try
        TranscodingStreams.startproc(codec, :read, err) === :ok || throw(CodecError(name, :decompress, codecmessage(err)))
        while true
            if outlen == length(out)
                outlen >= maxout && throw(LimitError(:max_block_bytes, outlen + 1, maxout, :max_block_bytes, :decode))
                out = growbuffer!(budget, out, min(max(2 * length(out), 1 << 12), maxout), outlen)
            end
            navail = to - inpos + 1
            Δin, Δout, status = GC.@preserve input out TranscodingStreams.process(codec,
                TranscodingStreams.Memory(navail > 0 ? pointer(input, inpos) : Ptr{UInt8}(0), UInt(max(navail, 0))),
                TranscodingStreams.Memory(pointer(out, outlen + 1), UInt(length(out) - outlen)), err)
            inpos += Δin
            outlen += Δout
            status === :end && return (inpos - from, out, outlen)
            status === :error && throw(CodecError(name, :decompress, codecmessage(err)))
            (Δin == 0 && Δout == 0 && navail <= 0) && throw(CodecError(name, :decompress, "truncated $name member"))
        end
    finally
        TranscodingStreams.finalize(codec)
    end
end

initialoutput(budget::Budget, inputlen::Int, maxout::Int) = (cap = min(max(4 * inputlen, 1 << 12), maxout); reserve!(budget, bytesbytes(cap)); Vector{UInt8}(undef, cap))

# ---- decompression ----------------------------------------------------------------------------------

"""
    decompressblock(name, codec, payload, limits, budget) -> Vector{UInt8}

The owned decompressed bytes of one block payload: every member decoded with its reported decoder
requirement checked against `max_codec_memory` and reserved, members counted, the payload exactly
exhausted, and the output bounded by `max_block_bytes`.
"""
function decompressblock(name::Symbol, ::NullCodec, payload::AbstractVector{UInt8}, limits::Limits, budget::Budget)
    addinput!(budget, length(payload))
    addmembers!(budget)
    reserve!(budget, bytesbytes(length(payload)))
    return Vector{UInt8}(payload)
end

function decompressblock(name::Symbol, ::DeflateReader, payload::AbstractVector{UInt8}, limits::Limits, budget::Budget)
    reserve!(budget, DEFLATE_DECODER_BYTES)
    out = initialoutput(budget, length(payload), limits.max_block_bytes)
    consumed, out, outlen = transcodemember!(:deflate, DeflateDecompressor(), payload, 1, length(payload), out, 0, limits.max_block_bytes, budget)
    release!(budget, DEFLATE_DECODER_BYTES)
    trailing = length(payload) - consumed
    trailing == 0 || throw(CodecError(:deflate, :decompress, "$trailing bytes after the final deflate block"))
    addinput!(budget, outlen)
    addmembers!(budget)
    return shrinkexact(budget, out, outlen)
end

function decompressblock(name::Symbol, ::SnappyCodec, payload::AbstractVector{UInt8}, limits::Limits, budget::Budget)
    n = length(payload)
    n >= 4 || throw(CodecError(:snappy, :decompress, "snappy block shorter than its 4-byte CRC"))
    datalen = n - 4
    result = Ref{Csize_t}(0)
    st = GC.@preserve payload Snappy.LibSnappy.snappy_uncompressed_length(pointer(payload), datalen, result)
    st == Snappy.LibSnappy.SNAPPY_OK || throw(CodecError(:snappy, :decompress, "invalid snappy data (status $st)"))
    m = Int(result[])
    m <= limits.max_block_bytes || throw(LimitError(:max_block_bytes, m, limits.max_block_bytes, :max_block_bytes, :decode))
    reserve!(budget, bytesbytes(m))
    out = Vector{UInt8}(undef, m)
    len = Ref{Csize_t}(m)
    st2 = GC.@preserve payload out Snappy.LibSnappy.snappy_uncompress(pointer(payload), datalen, pointer(out), len)
    (st2 == Snappy.LibSnappy.SNAPPY_OK && Int(len[]) == m) || throw(CodecError(:snappy, :decompress, "snappy decompression failed (status $st2)"))
    stored = (UInt32(payload[n - 3]) << 24) | (UInt32(payload[n - 2]) << 16) | (UInt32(payload[n - 1]) << 8) | UInt32(payload[n])
    crc32(out) == stored || throw(CodecError(:snappy, :decompress, "snappy CRC mismatch"))
    addinput!(budget, m)
    addmembers!(budget)
    return out
end

function decompressblock(name::Symbol, z::ZstdReader, payload::AbstractVector{UInt8}, limits::Limits, budget::Budget)
    total = length(payload)
    total > 0 || throw(CodecError(:zstandard, :decompress, "zstandard payload has no frame"))
    out = initialoutput(budget, total, limits.max_block_bytes)
    outlen = 0
    pos = 1
    members = 0
    while pos <= total
        rem = total - pos + 1
        fsz = zstd_framesize(payload, pos, rem)
        fsz === nothing && throw(CodecError(:zstandard, :decompress, "not a complete zstandard frame at payload byte $pos"))
        members = checked_add(members, 1)
        if zstd_skippable(payload, pos, rem)
            pos += fsz
            continue
        end
        est = zstd_frameestimate(payload, pos, rem)
        est === nothing && throw(CodecError(:zstandard, :decompress, "unreadable zstandard frame header at payload byte $pos"))
        est <= limits.max_codec_memory || throw(CodecError(:zstandard, :decompress, "the frame at payload byte $pos needs $est bytes of decoder memory; max_codec_memory is $(limits.max_codec_memory)"))
        reserve!(budget, est)
        consumed, out, outlen = transcodemember!(:zstandard, ZstdDecompressor(windowLogMax=z.windowlogmax), payload, pos, pos + fsz - 1, out, outlen, limits.max_block_bytes, budget)
        release!(budget, est)
        consumed == fsz || throw(CodecError(:zstandard, :decompress, "zstandard frame not exactly consumed"))
        pos += fsz
    end
    addinput!(budget, outlen)
    addmembers!(budget, members)
    return shrinkexact(budget, out, outlen)
end

decompressblock(name::Symbol, state, payload::AbstractVector{UInt8}, limits::Limits, budget::Budget) =
    throw(UnsupportedCodecError(String(name), get(EXTENSION_PACKAGES, name, nothing)))

# ---- the writing side --------------------------------------------------------------------------------

"""
    WriterCodec

A validated writer-side codec: the compressor factory, its workspace charge, and for zstandard the
selected frame `windowLog` (every emitted frame is verified against `max_codec_memory`).
"""
struct WriterCodec
    name::Symbol
    factory::Any
    workspace::Int
    windowlog::Int
end

"Validate `codec`/`level` for writing under `limits` (plan §4.9: a writer never emits a frame a reader with identical limits cannot decode)."
function writercodec(name::Symbol, level, limits::Limits)
    if name === :null
        level === nothing || throw(ArgumentError("the null codec takes no level"))
        return WriterCodec(:null, nothing, 0, 0)
    elseif name === :deflate
        lvl = level === nothing ? 6 : Int(level)
        0 <= lvl <= 9 || throw(ArgumentError("the deflate level must be 0–9, got $lvl"))
        return WriterCodec(:deflate, () -> DeflateCompressor(level=lvl), DEFLATE_ENCODER_BYTES, 0)
    elseif name === :snappy
        level === nothing || throw(ArgumentError("the snappy codec takes no level"))
        return WriterCodec(:snappy, nothing, 0, 0)
    elseif name === :zstandard
        lvl = level === nothing ? 3 : Int(level)
        1 <= lvl <= 22 || throw(ArgumentError("the zstandard level must be 1–22, got $lvl"))
        workspace = zstd_cstreamsize(lvl)                        # charged to the writer's ceiling at construction
        deflog = Int(zstd_cparams(lvl).windowLog)
        wlog = 0
        for L in 10:deflog
            zstd_dstreamsize(Int64(1) << L) <= limits.max_codec_memory && (wlog = L)
        end
        wlog == 0 && throw(LimitError(:max_codec_memory, zstd_dstreamsize(1 << 10), limits.max_codec_memory, :max_codec_memory, :encode))
        return WriterCodec(:zstandard, () -> ZstdCompressor(level=lvl, windowLog=Int32(wlog)), workspace, wlog)
    end
    haskey(EXTENSION_CODECS, name) && return EXTENSION_CODECS[name].writer(level, limits)::WriterCodec
    throw(UnsupportedCodecError(String(name), get(EXTENSION_PACKAGES, name, nothing)))
end

"Compress one block's encoded bytes (snappy appends the big-endian CRC32 of the uncompressed data)."
function compressblock(w::WriterCodec, bytes::Vector{UInt8})
    w.name === :null && return bytes
    if w.name === :snappy
        maxlen = Snappy.LibSnappy.snappy_max_compressed_length(UInt(length(bytes)))
        out = Vector{UInt8}(undef, Int(maxlen) + 4)
        len = Ref{Csize_t}(maxlen)
        st = GC.@preserve bytes out Snappy.LibSnappy.snappy_compress(pointer(bytes), length(bytes), pointer(out), len)
        st == Snappy.LibSnappy.SNAPPY_OK || throw(CodecError(:snappy, :compress, "snappy compression failed (status $st)"))
        c = crc32(bytes)
        m = Int(len[])
        out[m + 1] = UInt8(c >> 24); out[m + 2] = UInt8((c >> 16) & 0xff); out[m + 3] = UInt8((c >> 8) & 0xff); out[m + 4] = UInt8(c & 0xff)
        resize!(out, m + 4)
        return out
    end
    return TranscodingStreams.transcode(w.factory()::TranscodingStreams.Codec, bytes)
end

"Verify an emitted zstandard frame decodes under `max_codec_memory` (unreachable by construction; gated)."
function verifyframe(w::WriterCodec, block::Vector{UInt8}, limits::Limits)
    w.name === :zstandard || return nothing
    est = zstd_frameestimate(block, 1, length(block))
    (est === nothing || est > limits.max_codec_memory) && throw(CodecError(:zstandard, :compress,
        "the emitted frame needs $(est === nothing ? "unknown" : est) bytes of decoder memory; max_codec_memory is $(limits.max_codec_memory)"))
    return nothing
end

# ---- CRC32 (IEEE, reflected; snappy's checksum of the uncompressed data) -----------------------------

const CRC32_TABLE = let table = Vector{UInt32}(undef, 256)
    for i in 0:255
        c = UInt32(i)
        for _ in 1:8
            c = (c & 0x1) == 0x1 ? (c >> 1) ⊻ 0xedb88320 : c >> 1
        end
        table[i + 1] = c
    end
    table
end

function crc32(data::AbstractVector{UInt8}, crc::UInt32=UInt32(0))
    c = ~crc
    for b in data
        c = (c >> 8) ⊻ @inbounds CRC32_TABLE[((c ⊻ b) & 0xff) + 1]
    end
    return ~c
end
