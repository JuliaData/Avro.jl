# Decompress (and, for deflate, recompress) every block of a container file with Avro.jl's own block codec and
# with a bare one-shot library call into one reused buffer, libdeflate for deflate and ZSTD_decompressDCtx for
# zstandard, to show how much Avro.jl adds to the library's own time (not a reported measurement).
# usage: julia --project=julia --startup-file=no -t 1 profile/codecs.jl data/numeric-deflate.avro
using Pkg
const PROJECT = Base.active_project()
tmp = mktempdir()
Pkg.activate(tmp; io=devnull)
Pkg.add("libdeflate_jll"; io=devnull)
Pkg.activate(PROJECT; io=devnull)
push!(LOAD_PATH, tmp)
using Avro, libdeflate_jll, Printf

function varint(b, p)
    x = UInt64(0); s = 0
    while true
        c = b[p]; p += 1
        x |= UInt64(c & 0x7f) << s
        c < 0x80 && return (Int64(x >>> 1) ⊻ -Int64(x & 1), p)
        s += 7
    end
end

# The payloads of a container file's blocks, after the header (magic, metadata map, sync marker).
function payloads(b)
    p = 5
    while true
        n, p = varint(b, p)
        n == 0 && break
        n < 0 && ((_, p) = varint(b, p); n = -n)
        for _ in 1:n
            l, p = varint(b, p); p += l
            l, p = varint(b, p); p += l
        end
    end
    p += 16
    out = Vector{Vector{UInt8}}()
    while p <= length(b)
        _, p = varint(b, p)
        len, p = varint(b, p)
        push!(out, b[p:p+len-1]); p += len + 16
    end
    return out
end

avrojl(blocks) = sum(length(Avro.decompressblock(:deflate, x, Avro.Limits())) for x in blocks)

function oneshot(blocks)
    d = ccall((:libdeflate_alloc_decompressor, libdeflate), Ptr{Cvoid}, ())
    out = Vector{UInt8}(undef, 1 << 20)
    got, used = Ref{Csize_t}(0), Ref{Csize_t}(0)
    total = 0
    for x in blocks
        # the _ex form accepts bytes after the final deflate block, which fastavro leaves
        r = ccall((:libdeflate_deflate_decompress_ex, libdeflate), Cint,
                  (Ptr{Cvoid}, Ptr{UInt8}, Csize_t, Ptr{UInt8}, Csize_t, Ref{Csize_t}, Ref{Csize_t}),
                  d, x, length(x), out, length(out), used, got)
        r == 0 || error("libdeflate status $r")
        total += got[]
    end
    ccall((:libdeflate_free_decompressor, libdeflate), Cvoid, (Ptr{Cvoid},), d)
    return total
end

blocks = payloads(read(ARGS[1]))
if occursin("zstandard", ARGS[1])
    const ZSTD = parentmodule(Avro.ZstdDecompressor).LibZstd.libzstd
    avrojl_z(blocks) = sum(length(Avro.decompressblock(:zstandard, x, Avro.Limits())) for x in blocks)
    function oneshot_z(blocks)
        ctx = ccall((:ZSTD_createDCtx, ZSTD), Ptr{Cvoid}, ())
        out = Vector{UInt8}(undef, 1 << 20)
        total = 0
        for x in blocks
            n = ccall((:ZSTD_decompressDCtx, ZSTD), Csize_t, (Ptr{Cvoid}, Ptr{UInt8}, Csize_t, Ptr{UInt8}, Csize_t),
                      ctx, out, length(out), x, length(x))
            ccall((:ZSTD_isError, ZSTD), Cuint, (Csize_t,), n) == 0 || error("zstd error")
            total += n
        end
        ccall((:ZSTD_freeDCtx, ZSTD), Csize_t, (Ptr{Cvoid},), ctx)
        return total
    end
    @assert avrojl_z(blocks) == oneshot_z(blocks)
    for (name, f) in (("Avro.jl (CodecZstd)", avrojl_z), ("ZSTD_decompressDCtx", oneshot_z))
        st = @timed f(blocks)
        t = minimum(@elapsed(f(blocks)) for _ in 1:3)
        @printf("%-20s %.3f s for %d blocks, %.0f MB/s of output, %.0f MiB allocated\n", name, t, length(blocks),
                f(blocks) / t / 1e6, st.bytes / 2^20)
    end
    exit()
end
@assert avrojl(blocks) == oneshot(blocks)
for (name, f) in (("Avro.jl", avrojl), ("libdeflate", oneshot))
    t = minimum(@elapsed(f(blocks)) for _ in 1:3)
    @printf("%-18s %.3f s for %d blocks, %.0f MB/s of output\n", name, t, length(blocks), f(blocks) / t / 1e6)
end

# The same blocks compressed again at level 6: Avro.jl's writer path against a bare libdeflate call.
raw = [Avro.decompressblock(:deflate, x, Avro.Limits()) for x in blocks]
w = Avro.writercodec(:deflate, 6)
avrojl_c(raw) = sum(length(Avro.compressblock(w, x)) for x in raw)
function oneshot_c(raw)
    c = ccall((:libdeflate_alloc_compressor, libdeflate), Ptr{Cvoid}, (Cint,), 6)
    out = Vector{UInt8}(undef, 1 << 21)
    total = 0
    for x in raw
        total += ccall((:libdeflate_deflate_compress, libdeflate), Csize_t,
                       (Ptr{Cvoid}, Ptr{UInt8}, Csize_t, Ptr{UInt8}, Csize_t), c, x, length(x), out, length(out))
    end
    ccall((:libdeflate_free_compressor, libdeflate), Cvoid, (Ptr{Cvoid},), c)
    return total
end
for (name, f) in (("Avro.jl -6", avrojl_c), ("libdeflate -6", oneshot_c))
    t = minimum(@elapsed(f(raw)) for _ in 1:2)
    @printf("%-18s %.3f s, %.0f MB/s of input, %d bytes out\n", name, t, sum(length, raw) / t / 1e6, f(raw))
end
