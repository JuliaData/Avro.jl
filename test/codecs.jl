import CodecBzip2, CodecXz                       # activate the codec extensions
import CodecZstd, CodecZlib, TranscodingStreams, Zlib_jll

mutable struct CompressionReservationIO <: IO
    inner::IOBuffer
    writer::Any
    baseline::Int
    encoded::Int
    outputcharge::Int
    outputcapacity::Int
end

function CompressionReservationIO()
    return CompressionReservationIO(IOBuffer(), nothing, 0, 0, -1, -1)
end

function Base.isopen(io::CompressionReservationIO)
    return isopen(io.inner)
end

function Base.flush(io::CompressionReservationIO)
    return flush(io.inner)
end

function Base.write(io::CompressionReservationIO, b::UInt8)
    return Base.write(io.inner, b)
end

function Base.unsafe_write(io::CompressionReservationIO, p::Ptr{UInt8}, n::UInt)
    return Base.unsafe_write(io.inner, p, n)
end

function Base.write(io::CompressionReservationIO, bytes::Vector{UInt8})
    if io.writer !== nothing
        w = io.writer
        io.outputcharge = w.budget.reserved - io.baseline - Avro.bytesbytes(io.encoded)
        io.outputcapacity = Avro.capacity(bytes)
    end
    return Base.write(io.inner, bytes)
end

@testset "Codecs" begin
    L = Avro.Limits()
    data = reduce(vcat, [Vector{UInt8}("hello avro codec block $(i % 7) ") for i in 1:2000])
    function codecdec(name::Symbol, payload; limits=L)
        return Avro.withbudget(limits) do budget
            sym, rc = Avro.readercodec(String(name), limits, nothing)
            Avro.decompressblock(sym, rc, payload, limits, budget)
        end
    end

    function codecstats(name::Symbol, payload; limits=L)
        return Avro.withbudget(limits) do budget
            sym, rc = Avro.readercodec(String(name), limits, nothing)
            out = Avro.decompressblock(sym, rc, payload, limits, budget)
            return (out, budget.members, budget.input_bytes)
        end
    end

    function codecworkfailure(name::Symbol, payload)
        limits = Avro.Limits(max_values_per_byte=0, work_allowance=1, max_total_values=100)
        budget = Avro.Budget(limits; available=1 << 40)
        sym, rc = Avro.readercodec(String(name), limits, nothing)
        err = try
            Avro.decompressblock(sym, rc, payload, limits, budget)
            nothing
        catch e
            e
        end
        result = (err, budget.members, budget.values)
        Avro.close!(budget)
        return result
    end
    @testset "round trips (every codec, incl. empty)" begin
        @test :bzip2 in Avro.codecs() && :xz in Avro.codecs()
        for (name, level) in ((:null, nothing), (:deflate, nothing), (:deflate, 9), (:snappy, nothing),
                              (:zstandard, nothing), (:zstandard, 19), (:bzip2, nothing), (:bzip2, 1), (:xz, nothing), (:xz, 2))
            w = Avro.writercodec(name, level, L)
            block = Avro.compressblock(w, copy(data))
            Avro.verifyframe(w, block, L)
            @test codecdec(name, block) == data
            name === :null ? (@test block == data) : (@test length(block) < length(data))
            @test codecdec(name, Avro.compressblock(w, UInt8[])) == UInt8[]
        end
    end
    @testset "Writer reserves the compressor's output allocation" begin
        schema = Avro.schema(Vector{UInt8})
        rng = Random.Xoshiro(42)
        for name in (:deflate, :zstandard, :bzip2, :xz)
            sink = CompressionReservationIO()
            writer = Avro.Writer(sink, schema; codec=name, block_bytes=1 << 20)
            push!(writer, rand(rng, UInt8, 512 << 10))
            sink.writer = writer
            sink.baseline = writer.budget.reserved
            sink.encoded = writer.encoder.pos
            close(writer)
            @test sink.outputcharge >= Avro.bytesbytes(sink.outputcapacity)
        end
    end
    @testset "levels, workspaces and unsupported codecs" begin
        @test_throws ArgumentError Avro.writercodec(:deflate, 10, L)
        @test_throws ArgumentError Avro.writercodec(:zstandard, 0, L)
        @test_throws ArgumentError Avro.writercodec(:zstandard, 23, L)
        @test_throws ArgumentError Avro.writercodec(:snappy, 1, L)
        @test_throws ArgumentError Avro.writercodec(:null, 1, L)
        @test_throws ArgumentError Avro.writercodec(:bzip2, 0, L)
        @test_throws ArgumentError Avro.writercodec(:xz, 10, L)
        e = try; Avro.readercodec("lz4", L, nothing); nothing; catch err; err; end
        @test e isa Avro.UnsupportedCodecError && e.package === nothing
        e2 = try; Avro.writercodec(:brotli, nothing, L); nothing; catch err; err; end
        @test e2 isa Avro.UnsupportedCodecError
        @test Avro.writercodec(:zstandard, 22, L).workspace > 500 << 20           # ≈ 834 MB: the Writer's ceiling rejects it at construction
        @test_throws Avro.LimitError Avro.writercodec(:xz, 9, L)                  # ≈ 65 MB decoder requirement > 32 MiB
        big = Avro.Limits(max_codec_memory=1 << 30, max_total_bytes=8 << 30)
        @test Avro.writercodec(:xz, 9, big) isa Avro.WriterCodec
        @test Avro.writercodec(:zstandard, 22, big).windowlog >= 10
        @test 23 <= Avro.zstdwindowlogmax(L.max_codec_memory) <= 25               # ≈ 17 MB at 2^24 under the 32 MiB default
        @test Avro.zstdwindowlogmax(big.max_codec_memory) == 29                   # 2^30's estimate exceeds 1 GiB
        @test Avro.zstdwindowlogmax(3 << 30) == 31
    end
    @testset "zstandard members, skippable frames, window caps" begin
        w = Avro.writercodec(:zstandard, 3, L)
        f1 = Avro.compressblock(w, Vector{UInt8}("abc"))
        f2 = Avro.compressblock(w, Vector{UInt8}("defg"))
        skip = vcat(UInt8[0x50, 0x2a, 0x4d, 0x18], UInt8[0x04, 0x00, 0x00, 0x00], UInt8[1, 2, 3, 4])
        @test_throws Avro.CodecError codecdec(:zstandard, UInt8[])
        @test codecdec(:zstandard, vcat(f1, f2)) == Vector{UInt8}("abcdefg")
        dense = Avro.Limits(max_values_per_byte=1, work_allowance=1)
        one = Avro.compressblock(w, UInt8[0x01])
        @test codecdec(:zstandard, vcat(one, one); limits=dense) == UInt8[0x01, 0x01]
        @test codecstats(:zstandard, vcat(one, one)) == (UInt8[0x01, 0x01], 2, 2)
        @test codecdec(:zstandard, vcat(skip, f1, skip, f2, skip)) == Vector{UInt8}("abcdefg")
        @test codecdec(:zstandard, skip) == UInt8[]
        dense_skippable = repeat(UInt8[0x50, 0x2a, 0x4d, 0x18, 0x00, 0x00, 0x00, 0x00], 10)
        err, members, values = codecworkfailure(:zstandard, dense_skippable)
        @test err isa Avro.LimitError && members == values == 2
        @test_throws Avro.CodecError codecdec(:zstandard, vcat(f1, UInt8[0x01, 0x02]))
        @test_throws Avro.CodecError codecdec(:zstandard, f1[1:end - 1])
        @test_throws Avro.CodecError codecdec(:zstandard, skip[1:6])
        big24 = TranscodingStreams.transcode(CodecZstd.ZstdCompressor(level=3, windowLog=Int32(24)), zeros(UInt8, 1 << 20))
        tiny = Avro.Limits(max_codec_memory=16 << 20)
        e = try; codecdec(:zstandard, big24; limits=tiny); nothing; catch err; err; end
        @test e isa Avro.CodecError && occursin("decoder memory", e.msg)          # ≈ 17 MB estimate > 16 MiB cap
        @test codecdec(:zstandard, big24) == zeros(UInt8, 1 << 20)                # the 32 MiB default admits it
    end
    @testset "deflate and snappy strictness" begin
        w = Avro.writercodec(:deflate, nothing, L)
        block = Avro.compressblock(w, data)
        @test_throws Avro.CodecError codecdec(:deflate, vcat(block, UInt8[0x01]))
        @test_throws Avro.CodecError codecdec(:deflate, vcat(block, UInt8[0x01, 0x02, 0x03]))
        @test_throws Avro.CodecError codecdec(:deflate, vcat(block, UInt8[0x01, 0x02, 0x03, 0x04]))
        @test_throws Avro.CodecError codecdec(:deflate, block[1:end - 1])
        sn = Avro.writercodec(:snappy, nothing, L)
        sb = Avro.compressblock(sn, data)
        bad = copy(sb)
        bad[end] ⊻= 0x01
        e = try; codecdec(:snappy, bad); nothing; catch err; err; end
        @test e isa Avro.CodecError && occursin("CRC", e.msg)
        @test_throws Avro.CodecError codecdec(:snappy, UInt8[1, 2, 3])
        @test_throws Avro.CodecError codecdec(:snappy, UInt8[0x05, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
    end
    @testset "bzip2 and xz members and padding" begin
        bz = Avro.writercodec(:bzip2, nothing, L)
        b1 = Avro.compressblock(bz, Vector{UInt8}("abc"))
        b2 = Avro.compressblock(bz, Vector{UInt8}("defg"))
        @test_throws Avro.CodecError codecdec(:bzip2, UInt8[])
        @test codecdec(:bzip2, vcat(b1, b2)) == Vector{UInt8}("abcdefg")
        dense = Avro.Limits(max_values_per_byte=1, work_allowance=1)
        bone = Avro.compressblock(bz, UInt8[0x01])
        @test codecdec(:bzip2, vcat(bone, bone); limits=dense) == UInt8[0x01, 0x01]
        @test codecstats(:bzip2, vcat(bone, bone)) == (UInt8[0x01, 0x01], 2, 2)
        bzerr, bzmembers, bzvalues = codecworkfailure(:bzip2, repeat(bone, 10))
        @test bzerr isa Avro.LimitError && bzmembers == bzvalues == 2
        @test_throws Avro.CodecError codecdec(:bzip2, vcat(b1, UInt8[0x00]))
        @test_throws Avro.CodecError codecdec(:bzip2, b1[1:end - 2])
        xz = Avro.writercodec(:xz, nothing, L)
        x1 = Avro.compressblock(xz, Vector{UInt8}("abc"))
        x2 = Avro.compressblock(xz, Vector{UInt8}("defg"))
        @test_throws Avro.CodecError codecdec(:xz, UInt8[])
        @test_throws Avro.CodecError codecdec(:xz, zeros(UInt8, 4))
        @test codecdec(:xz, vcat(x1, x2)) == Vector{UInt8}("abcdefg")
        xone = Avro.compressblock(xz, UInt8[0x01])
        @test codecdec(:xz, vcat(xone, xone); limits=dense) == UInt8[0x01, 0x01]
        @test codecstats(:xz, vcat(xone, xone)) == (UInt8[0x01, 0x01], 2, 2)
        xzerr, xzmembers, xzvalues = codecworkfailure(:xz, repeat(xone, 10))
        @test xzerr isa Avro.LimitError && xzmembers == xzvalues == 2
        @test codecdec(:xz, vcat(x1, zeros(UInt8, 4), x2, zeros(UInt8, 8))) == Vector{UInt8}("abcdefg")
        @test codecdec(:xz, vcat(x1, zeros(UInt8, 4))) == Vector{UInt8}("abc")
        @test_throws Avro.CodecError codecdec(:xz, vcat(x1, zeros(UInt8, 3)))
        @test_throws Avro.CodecError codecdec(:xz, vcat(x1, UInt8[0x00, 0x00, 0x00, 0x01]))
        @test_throws Avro.CodecError codecdec(:xz, x1[1:end - 4])
    end
    @testset "failed decompression restores transient reservations" begin
        good = Dict{Symbol,Vector{UInt8}}()
        bad = Dict{Symbol,Vector{UInt8}}()
        for name in (:deflate, :snappy, :zstandard, :bzip2, :xz)
            good[name] = Avro.compressblock(Avro.writercodec(name, nothing, L), Vector{UInt8}("reservation"))
        end
        bad[:deflate] = good[:deflate][1:end - 1]
        snappybad = copy(good[:snappy])
        snappybad[end] = snappybad[end] ⊻ 0x01
        bad[:snappy] = snappybad
        bad[:zstandard] = vcat(good[:zstandard], UInt8[0x01, 0x02])
        bad[:bzip2] = vcat(good[:bzip2], UInt8[0x00])
        bad[:xz] = vcat(good[:xz], zeros(UInt8, 3))
        for name in (:deflate, :snappy, :zstandard, :bzip2, :xz)
            budget = Avro.Budget(L; available=1 << 40)
            sym, reader = Avro.readercodec(String(name), L, nothing)
            baseline = budget.reserved
            @test_throws Avro.CodecError Avro.decompressblock(sym, reader, bad[name], L, budget)
            @test budget.reserved == baseline
            out = Avro.decompressblock(sym, reader, good[name], L, budget)
            @test out == Vector{UInt8}("reservation")
            Avro.release!(budget, Avro.bytesbytes(length(out)))
            @test budget.reserved == baseline
            Avro.close!(budget)
        end
    end
    @testset "crc32 against zlib" begin
        rng = Random.Xoshiro(5)
        for n in (0, 1, 7, 100, 10_000)
            d = rand(rng, UInt8, n)
            @test Avro.crc32(d) == UInt32(ccall((:crc32, Zlib_jll.libz), Culong, (Culong, Ptr{UInt8}, Cuint), 0, d, length(d)))
        end
    end
    @testset "decompression bombs are capped" begin
        w = Avro.writercodec(:deflate, nothing, L)
        bomb = Avro.compressblock(w, zeros(UInt8, 4 << 20))
        @test length(bomb) < 8192
        @test_throws Avro.LimitError codecdec(:deflate, bomb; limits=Avro.Limits(max_block_bytes=1 << 20))
        @test codecdec(:deflate, bomb) == zeros(UInt8, 4 << 20)
        zb = Avro.compressblock(Avro.writercodec(:zstandard, nothing, L), zeros(UInt8, 4 << 20))
        @test_throws Avro.LimitError codecdec(:zstandard, zb; limits=Avro.Limits(max_block_bytes=1 << 20))
    end
end
