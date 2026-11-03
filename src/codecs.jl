# Block compression. Decompression stops at `Limits.max_bytes`; zstandard frames (skippable ones
# included) and bzip2/xz streams are decoded to the end of the payload, deflate ignores bytes after its
# final block, and snappy carries the big-endian CRC32 of the uncompressed data.

import CodecZlib
using CodecZlib: DeflateCompressor, DeflateDecompressor
using CodecZstd: ZstdCompressor, ZstdDecompressor
import Snappy
import TranscodingStreams

const BUILTIN_CODECS = (:null, :deflate, :snappy, :zstandard)
const EXTENSION_PACKAGES = Dict{Symbol,String}(:bzip2 => "CodecBzip2", :xz => "CodecXz")
const EXTENSION_CODECS = Dict{Symbol,Any}()   # name => (decompress = (payload, limits) -> bytes, writer = level -> WriterCodec)

"Register an extension codec's factories (called from the extension module's `__init__`)."
function registercodec!(name::Symbol, factories)
    EXTENSION_CODECS[name] = factories
    return nothing
end

"""
    Avro.codecs() -> Vector{Symbol}

The container codecs this process can read and write (`:bzip2` and `:xz` appear once their extension
packages are loaded).
"""
function codecs()
    return append!(collect(Symbol, BUILTIN_CODECS), sort!(collect(keys(EXTENSION_CODECS))))
end

"The canonical name of the codec named in `avro.codec` (`legacy=:avrojl1` reads `zstd` as zstandard)."
function readercodec(name::AbstractString, legacy::Union{Nothing,Symbol})
    legacy === :avrojl1 && name == "zstd" && return :zstandard
    for candidate in BUILTIN_CODECS
        name == String(candidate) && return candidate
    end
    for candidate in keys(EXTENSION_CODECS)
        name == String(candidate) && return candidate
    end
    for (candidate, package) in EXTENSION_PACKAGES
        name == String(candidate) && throw(UnsupportedCodecError(String(name), package))
    end
    throw(UnsupportedCodecError(String(name), nothing))
end

# ---- decompression ----------------------------------------------------------------------------------

function codecmessage(err::TranscodingStreams.Error)
    return TranscodingStreams.haserror(err) ? sprint(showerror, err[]) : "codec failure"
end

# An exact allocation: `resize!` may reserve more than `cap`.
function growbuffer(out::Vector{UInt8}, len::Int, cap::Int)
    bigger = Vector{UInt8}(undef, min(2 * length(out), cap))
    copyto!(bigger, 1, out, 1, len)
    return bigger
end

"""
Decompress `input` with a TranscodingStreams `codec` into at most `maxbytes` bytes. When a member ends
before the input does, the codec restarts on the remaining bytes if `multimember`; otherwise they are
ignored.
"""
function transcodemembers(name::Symbol, codec::TranscodingStreams.Codec, input::AbstractVector{UInt8}, maxbytes::Int;
                          multimember::Bool=true)
    n = length(input)
    out = Vector{UInt8}(undef, min(max(4 * n, 4096), maxbytes))
    probe = UInt8[0x00]                           # once `out` is full at `maxbytes`, any byte landing here is one too many
    outlen = 0
    inpos = 1
    err = TranscodingStreams.Error()
    TranscodingStreams.initialize(codec)
    try
        TranscodingStreams.startproc(codec, :read, err) === :ok || throw(CodecError(name, :decompress, codecmessage(err)))
        while true
            outlen == length(out) && outlen < maxbytes && (out = growbuffer(out, outlen, maxbytes))
            full = outlen == length(out)
            Δin, Δout, status = GC.@preserve input out probe TranscodingStreams.process(codec,
                TranscodingStreams.Memory(pointer(input, inpos), UInt(n - inpos + 1)),
                full ? TranscodingStreams.Memory(pointer(probe), UInt(1)) :
                       TranscodingStreams.Memory(pointer(out, outlen + 1), UInt(length(out) - outlen)), err)
            full && Δout > 0 && throw(LimitError(:max_bytes, outlen + 1, maxbytes))
            inpos += Δin
            outlen += Δout
            status === :error && throw(CodecError(name, :decompress, codecmessage(err)))
            if status === :end
                (inpos > n || !multimember) && return resize!(out, outlen)
                TranscodingStreams.startproc(codec, :read, err) === :ok || throw(CodecError(name, :decompress, codecmessage(err)))
            elseif Δin == 0 && Δout == 0
                throw(CodecError(name, :decompress, inpos > n ? "truncated $name data" : "$name codec made no progress"))
            end
        end
    finally
        TranscodingStreams.finalize(codec)
    end
end

"""
The owned decompressed bytes of one block payload. Every codec but null raises `LimitError(:max_bytes)`
rather than produce more than `limits.max_bytes` bytes; a null payload is returned as is (copied unless a
`Vector{UInt8}`).
"""
function decompressblock(name::Symbol, payload::AbstractVector{UInt8}, limits::Limits)
    name === :null && return payload isa Vector{UInt8} ? payload : Vector{UInt8}(payload)
    # Bytes after the final deflate block are ignored, as Java and Python readers do: the Apache
    # Python writer and fastavro 1.13.1 and earlier leave 3 bytes of the zlib checksum there.
    name === :deflate && return transcodemembers(:deflate, DeflateDecompressor(), payload, limits.max_bytes; multimember=false)
    # libzstd consumes a skippable frame itself and ends it as an empty member.
    name === :zstandard && return transcodemembers(:zstandard, ZstdDecompressor(), payload, limits.max_bytes)
    name === :snappy && return snappydecompress(payload, limits)
    return runtimecall(extensiondecompress, name, payload, limits)::Vector{UInt8}
end

function extensiondecompress(name::Symbol, payload::AbstractVector{UInt8}, limits::Limits)
    return EXTENSION_CODECS[name].decompress(payload, limits)::Vector{UInt8}
end

function snappydecompress(payload::AbstractVector{UInt8}, limits::Limits)
    n = length(payload)
    n >= 4 || throw(CodecError(:snappy, :decompress, "snappy block shorter than its 4-byte CRC"))
    result = Ref{Csize_t}(0)
    st = GC.@preserve payload Snappy.LibSnappy.snappy_uncompressed_length(pointer(payload), n - 4, result)
    st == Snappy.LibSnappy.SNAPPY_OK || throw(CodecError(:snappy, :decompress, "invalid snappy data (status $st)"))
    m = Int(result[])
    m <= limits.max_bytes || throw(LimitError(:max_bytes, m, limits.max_bytes))
    out = Vector{UInt8}(undef, m)
    len = Ref{Csize_t}(m)
    st = GC.@preserve payload out Snappy.LibSnappy.snappy_uncompress(pointer(payload), n - 4, pointer(out), len)
    (st == Snappy.LibSnappy.SNAPPY_OK && len[] == m) ||
        throw(CodecError(:snappy, :decompress, "snappy decompression failed (status $st)"))
    stored = UInt32(payload[n - 3]) << 24 | UInt32(payload[n - 2]) << 16 | UInt32(payload[n - 1]) << 8 | UInt32(payload[n])
    crc32(out) == stored || throw(CodecError(:snappy, :decompress, "snappy CRC mismatch"))
    return out
end

# ---- compression ------------------------------------------------------------------------------------

"A validated writer codec: a built-in codec's level, or an extension codec's compressor `factory()`."
struct WriterCodec
    name::Symbol
    level::Int
    factory::Any
end

function WriterCodec(name::Symbol, factory)
    return WriterCodec(name, 0, factory)
end

"Validate `level` for writing with codec `name` (`nothing` selects the codec's default level)."
function writercodec(name::Symbol, level)
    if name === :null || name === :snappy
        level === nothing || throw(ArgumentError("the $name codec takes no level"))
        return WriterCodec(name, nothing)
    elseif name === :deflate
        lvl = level === nothing ? 6 : Int(level)
        0 <= lvl <= 9 || throw(ArgumentError("the deflate level must be 0–9, got $lvl"))
        return WriterCodec(:deflate, lvl, nothing)
    elseif name === :zstandard
        lvl = level === nothing ? 3 : Int(level)
        1 <= lvl <= 22 || throw(ArgumentError("the zstandard level must be 1–22, got $lvl"))
        return WriterCodec(:zstandard, lvl, nothing)
    end
    haskey(EXTENSION_CODECS, name) && return runtimecall(extensionwriter, name, level)::WriterCodec
    throw(UnsupportedCodecError(String(name), get(EXTENSION_PACKAGES, name, nothing)))
end

function extensionwriter(name::Symbol, level)
    return EXTENSION_CODECS[name].writer(level)::WriterCodec
end

"Compress one block's encoded bytes (the null codec returns `bytes` itself)."
function compressblock(w::WriterCodec, bytes::Vector{UInt8})
    w.name === :null && return bytes
    w.name === :snappy && return snappycompress(bytes)
    w.name === :deflate && return transcodeblock(DeflateCompressor(level=w.level), bytes)
    w.name === :zstandard && return transcodeblock(ZstdCompressor(level=w.level), bytes)
    return runtimecall(extensioncompress, w, bytes)::Vector{UInt8}
end

function extensioncompress(w::WriterCodec, bytes::Vector{UInt8})
    return transcodeblock(w.factory(), bytes)
end

function transcodeblock(codec::TranscodingStreams.Codec, bytes::Vector{UInt8})
    TranscodingStreams.initialize(codec)
    try
        return transcode(codec, bytes)::Vector{UInt8}
    finally
        TranscodingStreams.finalize(codec)
    end
end

"Snappy-compress `bytes` and append the big-endian CRC32 of the uncompressed data."
function snappycompress(bytes::Vector{UInt8})
    maxlen = Int(Snappy.LibSnappy.snappy_max_compressed_length(length(bytes)))
    out = Vector{UInt8}(undef, maxlen + 4)
    len = Ref{Csize_t}(maxlen)
    st = GC.@preserve bytes out Snappy.LibSnappy.snappy_compress(pointer(bytes), length(bytes), pointer(out), len)
    st == Snappy.LibSnappy.SNAPPY_OK || throw(CodecError(:snappy, :compress, "snappy compression failed (status $st)"))
    m = Int(len[])
    c = crc32(bytes)
    for i in 1:4
        out[m + i] = (c >> (32 - 8i)) % UInt8
    end
    return resize!(out, m + 4)
end

# ---- CRC32 (IEEE, reflected; snappy's checksum of the uncompressed data), through zlib ---------------

function crc32(data::Vector{UInt8})
    isempty(data) && return UInt32(0)
    return GC.@preserve data UInt32(ccall((:crc32_z, CodecZlib.libz), Culong, (Culong, Ptr{UInt8}, Csize_t),
                                          0, pointer(data), length(data)) & 0xffffffff)
end
