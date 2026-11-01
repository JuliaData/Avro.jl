import CodecBzip2, CodecXz                       # activate the codec extensions
import CodecZstd, CodecZlib, TranscodingStreams, Zlib_jll

@testset "Codecs" begin
    L = Avro.Limits()
    data = reduce(vcat, [Vector{UInt8}("hello avro codec block $(i % 7) ") for i in 1:2000])
    function codecdec(name::Symbol, payload; limits=L)
        return Avro.decompressblock(Avro.readercodec(String(name), nothing), payload, limits)
    end
    @testset "round trips (every codec, incl. empty)" begin
        @test :bzip2 in Avro.codecs() && :xz in Avro.codecs()
        for (name, level) in ((:null, nothing), (:deflate, nothing), (:deflate, 9), (:snappy, nothing),
                              (:zstandard, nothing), (:zstandard, 19), (:bzip2, nothing), (:bzip2, 1), (:xz, nothing), (:xz, 2))
            w = Avro.writercodec(name, level)
            block = Avro.compressblock(w, copy(data))
            @test codecdec(name, block) == data
            name === :null ? (@test block == data) : (@test length(block) < length(data))
            @test codecdec(name, Avro.compressblock(w, UInt8[])) == UInt8[]
        end
    end
    @testset "levels and unsupported codecs" begin
        @test_throws ArgumentError Avro.writercodec(:deflate, 10)
        @test_throws ArgumentError Avro.writercodec(:zstandard, 0)
        @test_throws ArgumentError Avro.writercodec(:zstandard, 23)
        @test_throws ArgumentError Avro.writercodec(:snappy, 1)
        @test_throws ArgumentError Avro.writercodec(:null, 1)
        @test_throws ArgumentError Avro.writercodec(:bzip2, 0)
        @test_throws ArgumentError Avro.writercodec(:xz, 10)
        e = try; Avro.readercodec("lz4", nothing); nothing; catch err; err; end
        @test e isa Avro.UnsupportedCodecError && e.package === nothing
        e2 = try; Avro.writercodec(:brotli, nothing); nothing; catch err; err; end
        @test e2 isa Avro.UnsupportedCodecError
        @test Avro.writercodec(:xz, 9) isa Avro.WriterCodec
        @test Avro.writercodec(:zstandard, 22) isa Avro.WriterCodec
    end
    @testset "zstandard members, skippable frames, windows" begin
        w = Avro.writercodec(:zstandard, 3)
        f1 = Avro.compressblock(w, Vector{UInt8}("abc"))
        f2 = Avro.compressblock(w, Vector{UInt8}("defg"))
        skip = vcat(UInt8[0x50, 0x2a, 0x4d, 0x18], UInt8[0x04, 0x00, 0x00, 0x00], UInt8[1, 2, 3, 4])
        @test_throws Avro.CodecError codecdec(:zstandard, UInt8[])
        @test codecdec(:zstandard, vcat(f1, f2)) == Vector{UInt8}("abcdefg")
        one = Avro.compressblock(w, UInt8[0x01])
        @test codecdec(:zstandard, vcat(one, one)) == UInt8[0x01, 0x01]
        @test codecdec(:zstandard, repeat(one, 10)) == fill(0x01, 10)
        @test codecdec(:zstandard, vcat(skip, f1, skip, f2, skip)) == Vector{UInt8}("abcdefg")
        @test codecdec(:zstandard, skip) == UInt8[]
        @test codecdec(:zstandard, repeat(UInt8[0x50, 0x2a, 0x4d, 0x18, 0x00, 0x00, 0x00, 0x00], 10)) == UInt8[]
        @test_throws Avro.CodecError codecdec(:zstandard, vcat(f1, UInt8[0x01, 0x02]))
        @test_throws Avro.CodecError codecdec(:zstandard, f1[1:end - 1])
        @test_throws Avro.CodecError codecdec(:zstandard, skip[1:6])
        big24 = TranscodingStreams.transcode(CodecZstd.ZstdCompressor(level=3, windowLog=Int32(24)), zeros(UInt8, 1 << 20))
        @test codecdec(:zstandard, big24) == zeros(UInt8, 1 << 20)                # a 16 MiB window decodes under the defaults
    end
    @testset "deflate trailing bytes and snappy strictness" begin
        w = Avro.writercodec(:deflate, nothing)
        block = Avro.compressblock(w, data)
        # Python writers leave part of the zlib checksum after the final block; readers ignore it.
        @test codecdec(:deflate, vcat(block, UInt8[0x01])) == data
        @test codecdec(:deflate, vcat(block, UInt8[0x01, 0x02, 0x03])) == data
        @test codecdec(:deflate, vcat(block, UInt8[0x01, 0x02, 0x03, 0x04])) == data
        @test_throws Avro.CodecError codecdec(:deflate, block[1:end - 1])
        sn = Avro.writercodec(:snappy, nothing)
        sb = Avro.compressblock(sn, data)
        bad = copy(sb)
        bad[end] ⊻= 0x01
        e = try; codecdec(:snappy, bad); nothing; catch err; err; end
        @test e isa Avro.CodecError && occursin("CRC", e.msg)
        @test_throws Avro.CodecError codecdec(:snappy, UInt8[1, 2, 3])
        @test_throws Avro.CodecError codecdec(:snappy, UInt8[0x05, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
    end
    @testset "bzip2 and xz members and padding" begin
        bz = Avro.writercodec(:bzip2, nothing)
        b1 = Avro.compressblock(bz, Vector{UInt8}("abc"))
        b2 = Avro.compressblock(bz, Vector{UInt8}("defg"))
        @test_throws Avro.CodecError codecdec(:bzip2, UInt8[])
        @test codecdec(:bzip2, vcat(b1, b2)) == Vector{UInt8}("abcdefg")
        bone = Avro.compressblock(bz, UInt8[0x01])
        @test codecdec(:bzip2, vcat(bone, bone)) == UInt8[0x01, 0x01]
        @test codecdec(:bzip2, repeat(bone, 10)) == fill(0x01, 10)
        @test_throws Avro.CodecError codecdec(:bzip2, vcat(b1, UInt8[0x00]))
        @test_throws Avro.CodecError codecdec(:bzip2, b1[1:end - 2])
        xz = Avro.writercodec(:xz, nothing)
        x1 = Avro.compressblock(xz, Vector{UInt8}("abc"))
        x2 = Avro.compressblock(xz, Vector{UInt8}("defg"))
        @test_throws Avro.CodecError codecdec(:xz, UInt8[])
        @test_throws Avro.CodecError codecdec(:xz, zeros(UInt8, 4))
        @test codecdec(:xz, vcat(x1, x2)) == Vector{UInt8}("abcdefg")
        xone = Avro.compressblock(xz, UInt8[0x01])
        @test codecdec(:xz, vcat(xone, xone)) == UInt8[0x01, 0x01]
        @test codecdec(:xz, repeat(xone, 10)) == fill(0x01, 10)
        @test codecdec(:xz, vcat(x1, zeros(UInt8, 4), x2, zeros(UInt8, 8))) == Vector{UInt8}("abcdefg")
        @test codecdec(:xz, vcat(x1, zeros(UInt8, 4))) == Vector{UInt8}("abc")
        @test_throws Avro.CodecError codecdec(:xz, vcat(x1, zeros(UInt8, 3)))
        @test_throws Avro.CodecError codecdec(:xz, vcat(x1, UInt8[0x00, 0x00, 0x00, 0x01]))
        @test_throws Avro.CodecError codecdec(:xz, x1[1:end - 4])
    end
    @testset "a failed decompression leaves the codec state reusable" begin
        good = Dict{Symbol,Vector{UInt8}}()
        bad = Dict{Symbol,Vector{UInt8}}()
        for name in (:deflate, :snappy, :zstandard, :bzip2, :xz)
            good[name] = Avro.compressblock(Avro.writercodec(name, nothing), Vector{UInt8}("reservation"))
        end
        bad[:deflate] = good[:deflate][1:end - 1]
        snappybad = copy(good[:snappy])
        snappybad[end] = snappybad[end] ⊻ 0x01
        bad[:snappy] = snappybad
        bad[:zstandard] = vcat(good[:zstandard], UInt8[0x01, 0x02])
        bad[:bzip2] = vcat(good[:bzip2], UInt8[0x00])
        bad[:xz] = vcat(good[:xz], zeros(UInt8, 3))
        for name in (:deflate, :snappy, :zstandard, :bzip2, :xz)
            sym = Avro.readercodec(String(name), nothing)
            @test_throws Avro.CodecError Avro.decompressblock(sym, bad[name], L)
            @test Avro.decompressblock(sym, good[name], L) == Vector{UInt8}("reservation")
        end
    end
    @testset "crc32 against zlib" begin
        rng = Random.Xoshiro(5)
        for n in (0, 1, 7, 100, 10_000)
            d = rand(rng, UInt8, n)
            @test Avro.crc32(d) == UInt32(ccall((:crc32, Zlib_jll.libz), Culong, (Culong, Ptr{UInt8}, Cuint), 0, d, length(d)))
        end
    end
    @testset "decompression bombs stop at max_bytes" begin
        w = Avro.writercodec(:deflate, nothing)
        bomb = Avro.compressblock(w, zeros(UInt8, 4 << 20))
        @test length(bomb) < 8192
        bomberr = try; codecdec(:deflate, bomb; limits=Avro.Limits(max_bytes=1 << 20)); nothing; catch err; err; end
        @test bomberr isa Avro.LimitError && bomberr.limit === :max_bytes && bomberr.value == 1 << 20
        @test codecdec(:deflate, bomb) == zeros(UInt8, 4 << 20)
        @test codecdec(:deflate, bomb; limits=Avro.Limits(max_bytes=4 << 20)) == zeros(UInt8, 4 << 20)
        zb = Avro.compressblock(Avro.writercodec(:zstandard, nothing), zeros(UInt8, 4 << 20))
        @test_throws Avro.LimitError codecdec(:zstandard, zb; limits=Avro.Limits(max_bytes=1 << 20))
        for name in (:bzip2, :xz)
            nb = Avro.compressblock(Avro.writercodec(name, nothing), zeros(UInt8, 4 << 20))
            @test_throws Avro.LimitError codecdec(name, nb; limits=Avro.Limits(max_bytes=1 << 20))
        end
    end
end
