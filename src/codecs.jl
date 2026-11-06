# Block compression. Decompression stops at `Limits.max_bytes`; zstandard frames (skippable ones
# included) and bzip2/xz streams are decoded to the end of the payload, deflate ignores bytes after its
# final block, and snappy carries the big-endian CRC32 of the uncompressed data. Deflate and CRC32 run on
# libdeflate.

using libdeflate_jll: libdeflate
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
    TranscodingStreams.haserror(err) || return "codec failure"
    e = err[]
    msg = e isa ErrorException ? e.msg : nothing           # `msg` is declared `AbstractString`
    return msg isa String ? msg : "codec failure"
end

# An exact allocation: `resize!` may reserve more than `cap`.
function growbuffer(out::Vector{UInt8}, len::Int, cap::Int)
    bigger = Vector{UInt8}(undef, min(2 * length(out), cap))
    copyto!(bigger, 1, out, 1, len)
    return bigger
end

"""
Decompress `input` with a TranscodingStreams `codec` into `scratch[]`, which grows to at most `maxbytes` bytes, and
return the decompressed length. When a member ends before the input does, the codec restarts on the remaining
bytes if `multimember`; otherwise they are ignored.
"""
function transcodemembers!(scratch::Base.RefValue{Vector{UInt8}}, name::Symbol, codec::TranscodingStreams.Codec,
                           input::AbstractVector{UInt8}, maxbytes::Int; multimember::Bool=true)
    n = length(input)
    out = scratch[]
    start = min(max(4 * n, 4096), maxbytes)
    start <= length(out) <= maxbytes || (out = Vector{UInt8}(undef, start))
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
                if inpos > n || !multimember
                    scratch[] = out
                    return outlen
                end
                TranscodingStreams.startproc(codec, :read, err) === :ok || throw(CodecError(name, :decompress, codecmessage(err)))
            elseif Δin == 0 && Δout == 0
                throw(CodecError(name, :decompress, inpos > n ? "truncated $name data" : "$name codec made no progress"))
            end
        end
    finally
        TranscodingStreams.finalize(codec)
    end
end

"`transcodemembers!` into a new vector of exactly the decompressed bytes."
function transcodemembers(name::Symbol, codec::TranscodingStreams.Codec, input::AbstractVector{UInt8}, maxbytes::Int;
                          multimember::Bool=true)
    scratch = Ref(UInt8[])
    n = transcodemembers!(scratch, name, codec, input, maxbytes; multimember=multimember)
    return resize!(scratch[], n)
end

"The owned decompressed bytes of one block payload (`decompressblock!`); a null payload in a `Vector{UInt8}` is returned as is."
function decompressblock(name::Symbol, payload::AbstractVector{UInt8}, limits::Limits)
    name === :null && payload isa Vector{UInt8} && return payload
    scratch = Ref(UInt8[])
    n = decompressblock!(scratch, name, payload, limits)
    return resize!(scratch[], n)
end

"""
Decompress one block payload into `scratch[]`, which is replaced by a larger vector when it is too small, and return
the decompressed length: the block is `scratch[][1:n]`. Every codec but null raises `LimitError(:max_bytes)` rather
than produce more than `limits.max_bytes` bytes; a null payload is copied.
"""
function decompressblock!(scratch::Base.RefValue{Vector{UInt8}}, name::Symbol, payload::AbstractVector{UInt8}, limits::Limits)
    if name === :null
        n = length(payload)
        length(scratch[]) < n && (scratch[] = Vector{UInt8}(undef, n))
        copyto!(scratch[], 1, payload, 1, n)
        return n
    end
    name === :deflate && return inflateblock!(scratch, payload, limits.max_bytes)
    # libzstd consumes a skippable frame itself and ends it as an empty member.
    name === :zstandard && return transcodemembers!(scratch, :zstandard, ZstdDecompressor(), payload, limits.max_bytes)
    name === :snappy && return snappydecompress!(scratch, payload, limits)
    out = runtimecall(extensiondecompress, name, payload, limits)::Vector{UInt8}
    scratch[] = out
    return length(out)
end

const LIBDEFLATE_SUCCESS = Cint(0)
const LIBDEFLATE_INSUFFICIENT_SPACE = Cint(3)

# A raw DEFLATE payload inflated into `scratch[]` in one call, which is retried in a buffer twice the size, up to
# `maxbytes`, while the output does not fit. libdeflate stops at the final block, so bytes after it are ignored, as
# Java and Python readers ignore them: the Apache Python writer and fastavro 1.13.1 and earlier leave 3 bytes of
# the zlib checksum there.
function inflateblock!(scratch::Base.RefValue{Vector{UInt8}}, payload::AbstractVector{UInt8}, maxbytes::Int)
    out = scratch[]
    start = min(max(4 * length(payload), 4096), maxbytes)
    start <= length(out) <= maxbytes || (out = Vector{UInt8}(undef, start))
    d = ccall((:libdeflate_alloc_decompressor, libdeflate), Ptr{Cvoid}, ())
    d == C_NULL && throw(OutOfMemoryError())
    try
        while true
            used, n = Ref{Csize_t}(0), Ref{Csize_t}(0)
            st = GC.@preserve payload out ccall((:libdeflate_deflate_decompress_ex, libdeflate), Cint,
                (Ptr{Cvoid}, Ptr{UInt8}, Csize_t, Ptr{UInt8}, Csize_t, Ref{Csize_t}, Ref{Csize_t}),
                d, pointer(payload), length(payload), pointer(out), length(out), used, n)
            if st == LIBDEFLATE_SUCCESS
                scratch[] = out
                return Int(n[])
            end
            st == LIBDEFLATE_INSUFFICIENT_SPACE || throw(CodecError(:deflate, :decompress, "invalid deflate data"))
            length(out) < maxbytes || throw(LimitError(:max_bytes, maxbytes + 1, maxbytes))
            out = Vector{UInt8}(undef, min(2 * length(out), maxbytes))
        end
    finally
        ccall((:libdeflate_free_decompressor, libdeflate), Cvoid, (Ptr{Cvoid},), d)
    end
end

function extensiondecompress(name::Symbol, payload::AbstractVector{UInt8}, limits::Limits)
    return EXTENSION_CODECS[name].decompress(payload, limits)::Vector{UInt8}
end

function snappydecompress!(scratch::Base.RefValue{Vector{UInt8}}, payload::AbstractVector{UInt8}, limits::Limits)
    n = length(payload)
    n >= 4 || throw(CodecError(:snappy, :decompress, "snappy block shorter than its 4-byte CRC"))
    result = Ref{Csize_t}(0)
    st = GC.@preserve payload Snappy.LibSnappy.snappy_uncompressed_length(pointer(payload), n - 4, result)
    st == Snappy.LibSnappy.SNAPPY_OK || throw(CodecError(:snappy, :decompress, "invalid snappy data (status $st)"))
    m = Int(result[])
    m <= limits.max_bytes || throw(LimitError(:max_bytes, m, limits.max_bytes))
    length(scratch[]) < m && (scratch[] = Vector{UInt8}(undef, m))
    out = scratch[]
    len = Ref{Csize_t}(m)
    st = GC.@preserve payload out Snappy.LibSnappy.snappy_uncompress(pointer(payload), n - 4, pointer(out), len)
    (st == Snappy.LibSnappy.SNAPPY_OK && len[] == m) ||
        throw(CodecError(:snappy, :decompress, "snappy decompression failed (status $st)"))
    stored = UInt32(payload[n - 3]) << 24 | UInt32(payload[n - 2]) << 16 | UInt32(payload[n - 1]) << 8 | UInt32(payload[n])
    crc32(out, m) == stored || throw(CodecError(:snappy, :decompress, "snappy CRC mismatch"))
    return m
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
    w.name === :deflate && return deflateblock(bytes, w.level)
    w.name === :zstandard && return transcodeblock(ZstdCompressor(level=w.level), bytes)
    return runtimecall(extensioncompress, w, bytes)::Vector{UInt8}
end

function extensioncompress(w::WriterCodec, bytes::Vector{UInt8})
    return transcodeblock(w.factory(), bytes)
end

"Raw-DEFLATE-compress one block at `level` (0–9)."
function deflateblock(bytes::Vector{UInt8}, level::Int)
    c = ccall((:libdeflate_alloc_compressor, libdeflate), Ptr{Cvoid}, (Cint,), level)
    c == C_NULL && throw(OutOfMemoryError())
    try
        bound = Int(ccall((:libdeflate_deflate_compress_bound, libdeflate), Csize_t, (Ptr{Cvoid}, Csize_t), c, length(bytes)))
        out = Vector{UInt8}(undef, bound)
        n = GC.@preserve bytes out ccall((:libdeflate_deflate_compress, libdeflate), Csize_t,
            (Ptr{Cvoid}, Ptr{UInt8}, Csize_t, Ptr{UInt8}, Csize_t), c, pointer(bytes), length(bytes), pointer(out), bound)
        n == 0 && throw(CodecError(:deflate, :compress, "deflate compression failed"))
        return resize!(out, Int(n))
    finally
        ccall((:libdeflate_free_compressor, libdeflate), Cvoid, (Ptr{Cvoid},), c)
    end
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

# ---- CRC32 (IEEE, reflected; snappy's checksum of the uncompressed data), through libdeflate ---------

function crc32(data::Vector{UInt8}, n::Int=length(data))
    return GC.@preserve data ccall((:libdeflate_crc32, libdeflate), UInt32, (UInt32, Ptr{UInt8}, Csize_t), 0, pointer(data), n)
end
