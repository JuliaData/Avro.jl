# a sink that fails after a fixed number of bytes (writer failure injection)
mutable struct FailIO <: IO
    const io::IOBuffer
    const failafter::Int
    written::Int
end
FailIO(n::Int) = FailIO(IOBuffer(), n, 0)
function Base.write(f::FailIO, b::UInt8)
    f.written += 1
    f.written > f.failafter && throw(ErrorException("sink failure injected at byte $(f.written)"))
    return write(f.io, b)
end
function Base.unsafe_write(f::FailIO, p::Ptr{UInt8}, n::UInt)
    f.written += Int(n)
    f.written > f.failafter && throw(ErrorException("sink failure injected at byte $(f.written)"))
    return unsafe_write(f.io, p, n)
end
Base.flush(f::FailIO) = flush(f.io)

@noinline function abandonedwriter(io, schema)
    writer = Avro.Writer(io, schema; codec=:zstandard)
    return WeakRef(writer)
end

@testset "Container files" begin
    P = Avro.parseschema
    L = Avro.Limits()
    gen = joinpath(FIXTURES, "generated")
    readall(src; kw...) = Avro.Reader(r -> collect(Avro.eachdatum(r)), src; kw...)
    @testset "every fixture file decodes to its Java tojson expectations" begin
        cases = Tuple{String,String}[]
        for f in sort(readdir(joinpath(gen, "roots"); join=true))
            endswith(f, ".avro") && push!(cases, (f, replace(replace(basename(f), r"-[a-z]+\.avro$" => ""), r"-fastavro$" => "")))
        end
        for f in sort(readdir(joinpath(gen, "data"); join=true))
            endswith(f, ".avro") && push!(cases, (f, replace(replace(basename(f), r"-[a-z]+\.avro$" => ""), r"-fastavro$" => "")))
        end
        @test length(cases) > 150
        checked = 0
        for (file, base) in cases
            if endswith(basename(file), "-fastavro-deflate.avro")
                err = try
                    Avro.Reader(r -> collect(Avro.eachdatum(r)), file)
                    nothing
                catch e
                    e
                end
                @test err isa Avro.CodecError && occursin("bytes after the final deflate block", err.msg)
                checked += 1
                continue
            end
            dir = dirname(file)
            jsonl = joinpath(dir, base * ".jsonl")
            isfile(jsonl) || (jsonl = joinpath(dirname(dir), "data", base * ".jsonl"))
            isfile(jsonl) || continue
            vs = Avro.Reader(file) do r
                (collect(Avro.eachdatum(r)), Avro.writerschema(r))
            end
            values, ws = vs
            expected = [Avro.fromjson(ws, line) for line in readlines(jsonl) if !isempty(line)]
            if isempty(expected) && Avro.minsize(ws) == 0
                @test !isempty(values) && all(v -> isequal(v, Avro.decode(ws, UInt8[])), values)   # avro-tools' tojson prints nothing for zero-byte datums
            elseif (occursin("-fastavro-", basename(file)) || endswith(basename(file), "-xz.avro")) && ws isa Avro.RecordSchema &&
                   any(f -> f.schema isa Avro.UnionSchema && Avro.nullablebranch(f.schema) == 0, ws.fields)
                # the committed -xz.avro data (like -fastavro-*) went through fastavro, which reselects union branches
                # fastavro rewrote Java's data and reselects union branches (enum → string, float/long → double);
                # it is an oracle only where branch identity is preserved (plan §8.5)
                @test length(values) == length(expected)
                keep = [!(f.schema isa Avro.UnionSchema && Avro.nullablebranch(f.schema) == 0) for f in ws.fields]
                @test all(all(!keep[j] || isequal(values[i][j], expected[i][j]) for j in eachindex(ws.fields)) for i in eachindex(values))
            else
                @test length(values) == length(expected)
                @test all(isequal.(values, expected))
            end
            checked += 1
        end
        @test checked > 150
    end
    @testset "apache fixtures" begin
        wlines = readlines(joinpath(FIXTURES, "apache", "weather.json"))
        for name in ("weather.avro", "weather-deflate.avro", "weather-snappy.avro", "weather-zstd.avro", "weather-sorted.avro")
            values, ws = Avro.Reader(joinpath(FIXTURES, "apache", name)) do r
                (collect(Avro.eachdatum(r)), Avro.writerschema(r))
            end
            @test length(values) == length(wlines)
            expected = [Avro.fromjson(ws, l) for l in wlines]
            if name == "weather-sorted.avro"
                @test sort!([Avro.tojson(ws, v) for v in values]) == sort!([Avro.tojson(ws, v) for v in expected])
            else
                @test all(isequal.(values, expected))
            end
        end
        r = Avro.Reader(joinpath(FIXTURES, "apache", "syncInMeta.avro"))
        @test length(collect(Avro.eachdatum(r))) >= 1                        # a sync marker embedded in the metadata parses
        close(r)
        for sub in ("simple", "withUnion")
            f = joinpath(FIXTURES, "apache", "schemas", sub, "data.avro")
            @test length(readall(f)) >= 1
        end
    end
    @testset "write → read round trips (every codec, block boundaries, metadata)" begin
        rows = [(a=Int64(i), b="row $i", c=i / 2, flag=isodd(i)) for i in 1:503]
        for codecname in Avro.codecs()
            io = Avro.tobuffer(rows; codec=codecname, block_bytes=256, metadata=Dict("who" => Vector{UInt8}("me"), "bin" => UInt8[0xff, 0x00, 0xfe]))
            r = Avro.Reader(io)
            @test Avro.codec(r) === codecname
            @test Vector{UInt8}("me") == Avro.metadata(r)["who"]
            @test Avro.metadata(r)["bin"] == UInt8[0xff, 0x00, 0xfe]                        # non-UTF-8 metadata values round-trip
            @test Avro.metadata(r)["avro.codec"] == Vector{UInt8}(String(codecname))
            @test Avro.sync(r) isa NTuple{16,UInt8}
            got = collect(Avro.eachdatum(r))
            close(r)
            @test length(got) == length(rows)
            @test all(got[i].a == rows[i].a && got[i].b == rows[i].b && got[i].c == rows[i].c && got[i].flag == rows[i].flag for i in eachindex(rows))
            blocks = Avro.Reader(rr -> collect(Avro.eachblock(rr)), Avro.tobuffer(rows; codec=codecname, block_bytes=256))
            @test sum(first, blocks) == length(rows) && length(blocks) > 3
        end
        # a held block's bytes stay valid after iteration advances
        io = Avro.tobuffer(rows; block_bytes=256)
        r = Avro.Reader(io)
        it = Avro.eachblock(r)
        (c1, b1), _ = iterate(it)
        rest = collect(it)
        @test !isempty(rest)
        d = Avro.Budget(L; available=1 << 40)
        Avro.addinput!(d, length(b1))
        dec = Avro.Decoder(b1, d)
        @test Avro.decode(Avro.readplan(Avro.writerschema(r)), dec).a == 1
        close(r)
        # partitions become block boundaries
        parts = Tables.partitioner([rows[1:100], rows[101:503]])
        blocks = Avro.Reader(rr -> collect(Avro.eachblock(rr)), Avro.tobuffer(parts))
        @test first.(blocks[1:1]) == [100]
        # header-only file (zero datums) reads back
        empty = Avro.tobuffer(rows[1:0])
        @test readall(empty) == []
        # explicit schema and non-record roots through the Writer
        arr = P("{\"type\":\"array\",\"items\":\"string\"}")
        io2 = IOBuffer()
        w = Avro.Writer(io2, arr; sync=collect(UInt8(1):UInt8(16)))
        push!(w, ["a", "b"])
        push!(w, String[])
        close(w)
        seekstart(io2)
        r2 = Avro.Reader(io2)
        @test Avro.sync(r2) === ntuple(i -> UInt8(i), 16)
        @test isequal(collect(Avro.eachdatum(r2)), [["a", "b"], String[]])
        close(r2)
    end
    @testset "sources: path, mmap=false stream, IO, bytes" begin
        rows = [(x=Int64(i),) for i in 1:100]
        buffered = Avro.tobuffer(rows)
        buffered_reader = Avro.Reader(buffered)
        @test buffered_reader.source.buf === buffered.data
        @test buffered_reader.source.stop == buffered.size
        @test [v.x for v in Avro.eachdatum(buffered_reader)] == collect(1:100)
        close(buffered_reader)
        path = joinpath(mktempdir(), "t.avro")
        Avro.write(path, rows; codec=:deflate, block_bytes=64)
        expected = collect(1:100)
        @test [v.x for v in readall(path)] == expected
        @test [v.x for v in readall(path; mmap=false)] == expected
        open(path) do io
            @test [v.x for v in readall(io)] == expected
        end
        @test [v.x for v in readall(read(path))] == expected
        got = open(io -> [v.x for v in readall(io)], path)
        @test got == expected
    end
    @testset "legacy 1.x files and decimal byte order" begin
        leg = joinpath(gen, "legacy1x")
        @test_throws Avro.SchemaError readall(joinpath(leg, "avrojl112-null.avro"))               # 1.x wrote nameless fixed schemas
        @test_throws Avro.SchemaError readall(joinpath(leg, "avrojl112-zstd.avro"))
        vals = @test_logs (:warn, r"legacy") match_mode=:any readall(joinpath(leg, "avrojl112-null.avro"); legacy=:avrojl1, decimal_byteorder=:little)
        @test length(vals) == 2 && vals[1].a == 1 && vals[1].b == "x" && vals[2].b == "yy"
        @test vals[1].dec == Avro.Decimal(12345, 2) && vals[2].dec == Avro.Decimal(-123, 2)
        @test vals[1].u == UUID(0x123e4567e89b12d3a456426614174000)
        zvals = readall(joinpath(leg, "avrojl112-zstd.avro"); legacy=:avrojl1, decimal_byteorder=:little)
        @test length(zvals) == 2 && zvals[1].dec == Avro.Decimal(12345, 2)
        e = try; readall(joinpath(leg, "avrojl112-deflate.avro"); legacy=:avrojl1); nothing; catch err; err; end
        @test e isa Avro.DataError && occursin("precision", e.msg)                                # a :big misread of 1.x decimals fails validation rather than yielding garbage
    end
    @testset "truncation, corruption, crafted headers" begin
        rows = [(x=Int64(i),) for i in 1:10]
        buf = take!(Avro.tobuffer(rows; sync=collect(UInt8(17):UInt8(32))))
        sync = collect(UInt8(17):UInt8(32))
        headerend = findfirst(i -> buf[i:i + 15] == sync, 1:length(buf) - 15) + 15   # the last byte of the header sync
        @test headerend > 20
        for cut in (1, 3, headerend - 1, headerend + 1, length(buf) - 1)
            @test_throws Avro.DataError readall(buf[1:cut])
        end
        bad = copy(buf)
        bad[end] ⊻= 0x01                                                                          # the final sync byte
        e = try; readall(bad); nothing; catch err; err; end
        @test e isa Avro.DataError && occursin("sync", e.msg)
        badmagic = copy(buf)
        badmagic[1] = UInt8('X')
        @test_throws Avro.DataError readall(badmagic)
        header = buf[1:headerend]
        varint(n) = Avro.encode(P("\"long\""), n)
        @test_throws Avro.DataError readall(vcat(header, varint(-1)))                             # negative count
        @test_throws Avro.DataError readall(vcat(header, varint(1), varint(-2)))                  # negative size
        @test_throws Avro.DataError readall(vcat(header, varint(typemin(Int64))))
        @test readall(vcat(header, varint(0), varint(0), sync)) == []                             # a zero-count block is valid
        trailing = vcat(header, varint(1), varint(length(Avro.encode(P("{\"type\":\"record\",\"name\":\"Record\",\"fields\":[{\"name\":\"x\",\"type\":\"long\"}]}"), (x=Int64(7),))) + 1),
                        Avro.encode(P("{\"type\":\"record\",\"name\":\"Record\",\"fields\":[{\"name\":\"x\",\"type\":\"long\"}]}"), (x=Int64(7),)), UInt8[0x00], sync)
        lv = @test_logs (:warn, r"legacy") match_mode=:any Avro.Reader(r -> collect(Avro.eachdatum(r)), trailing; legacy=:avrojl1)
        @test length(lv) == 1 && lv[1].x == 7                                                     # the null-codec trailing tolerance
        toobig = vcat(header, varint(1), varint(Int64(L.max_block_bytes) + 1))
        @test_throws Avro.LimitError readall(toobig)
        # a block that declares more datums than it holds, and one with trailing bytes
        one = Avro.encode(P("{\"type\":\"record\",\"name\":\"Record\",\"fields\":[{\"name\":\"x\",\"type\":\"long\"}]}"), (x=Int64(7),))
        @test_throws Avro.DataError readall(vcat(header, varint(2), varint(length(one)), one, sync))
        @test_throws Avro.DataError readall(vcat(header, varint(1), varint(length(one) + 1), one, UInt8[0x00], sync))
        # no avro.schema
        noschema = vcat(collect(b"Obj\x01"), varint(0), collect(UInt8(1):UInt8(16)))
        @test_throws Avro.DataError readall(noschema)
        key = Vector{UInt8}("avro.schema")
        value = Vector{UInt8}("\"null\"")
        pair = vcat(varint(length(key)), key, varint(length(value)), value)
        sizedmetadata(n) = vcat(collect(b"Obj\x01"), varint(-1), varint(n), pair, varint(0), sync)
        @test readall(sizedmetadata(length(pair))) == []
        for declared in (0, length(pair) - 1, length(pair) + 1)
            @test_throws Avro.DataError readall(sizedmetadata(declared))
        end
        encodedpair(k, v) = vcat(varint(sizeof(k)), Vector{UInt8}(k), varint(length(v)), v)
        metadata_pairs = Vector{UInt8}[encodedpair("avro.schema", value)]
        for i in 1:64
            push!(metadata_pairs, encodedpair("key$(lpad(i, 4, '0'))", UInt8[]))
        end
        push!(metadata_pairs, encodedpair("key0001", UInt8[]))
        duplicate_header = vcat(collect(b"Obj\x01"), varint(length(metadata_pairs)),
                                reduce(vcat, metadata_pairs), varint(0), sync)
        comparison_limits = Avro.Limits(max_compare_bytes_per_byte=0, work_allowance=100)
        duplicate_error = try
            Avro.Reader(duplicate_header; limits=comparison_limits)
            nothing
        catch err
            err
        end
        @test duplicate_error isa Avro.LimitError && duplicate_error.limit == :max_compare_bytes_per_byte
    end
    @testset "writer failure injection, poisoning, atomic paths" begin
        rows = [(x=Int64(i),) for i in 1:10]
        s = Avro.schema(typeof(rows[1]))
        # a sink that fails after N bytes
        io = FailIO(200)
        w = Avro.Writer(io, s)
        e = try
            for r in rows
                push!(w, r)
                flush(w)
            end
            nothing
        catch err
            err
        end
        @test e !== nothing
        @test_throws Avro.WriterClosedError push!(w, rows[1])
        close(w)
        @test_throws Avro.WriterClosedError push!(w, rows[1])
        # a datum the schema rejects leaves the pending block intact
        io2 = IOBuffer()
        w2 = Avro.Writer(io2, s)
        push!(w2, (x=Int64(1),))
        @test_throws Avro.EncodeError push!(w2, (x="nope",))
        push!(w2, (x=Int64(2),))
        close(w2)
        seekstart(io2)
        @test [v.x for v in readall(io2)] == [1, 2]
        pending0 = @atomic Avro.GUARD.pending
        abandoned_io = IOBuffer()
        abandoned = abandonedwriter(abandoned_io, s)
        GC.gc(true)
        GC.gc(true)
        @test abandoned.value === nothing
        @test (@atomic Avro.GUARD.pending) == pending0
        @test isopen(abandoned_io)
        # atomic path: the destination is untouched until close, replaced on close, kept on abort
        dir = mktempdir()
        dest = joinpath(dir, "out.avro")
        Base.write(dest, "old")
        w3 = Avro.Writer(dest, s)
        push!(w3, (x=Int64(1),))
        @test read(dest, String) == "old"
        close(w3)
        @test [v.x for v in readall(dest)] == [1]
        w4 = Avro.Writer(dest, s)
        push!(w4, (x=Int64(9),))
        close(w4; abort=true)
        @test [v.x for v in readall(dest)] == [1]
        @test length(readdir(dir)) == 1                                                          # the temp file is gone
        # non-atomic in-place and fsync
        dest2 = joinpath(dir, "out2.avro")
        w5 = Avro.Writer(dest2, s; atomic=false, fsync=true)
        push!(w5, (x=Int64(4),))
        close(w5)
        @test [v.x for v in readall(dest2)] == [4]
        # reserved metadata and option validation
        @test_throws ArgumentError Avro.Writer(IOBuffer(), s; metadata=Dict("avro.codec" => UInt8[]))
        @test_throws ArgumentError Avro.Writer(IOBuffer(), s; sync=UInt8[1, 2])
        @test_throws ArgumentError Avro.Writer(IOBuffer(), s; block_bytes=0)
        @test_throws ArgumentError Avro.Writer(IOBuffer(), s; codec=:snappy, level=3)
        @test_throws Avro.LimitError Avro.Writer(IOBuffer(), s; codec=:zstandard, level=22)       # ≈ 834 MB workspace > the 256 MiB ceiling
        rep = P("{\"type\":\"record\",\"name\":\"B\",\"fields\":[{\"name\":\"bad-name\",\"type\":\"int\"}]}"; allow_invalid_names=true)
        @test_throws ArgumentError Avro.Writer(IOBuffer(), rep)
        @test Avro.Writer(IOBuffer(), rep; allow_invalid_names=true) isa Avro.Writer
    end
    @testset "the million-empty-string block and output-estimate caps" begin
        arr = P("{\"type\":\"array\",\"items\":\"string\"}")
        io = IOBuffer()
        w = Avro.Writer(io, arr)
        push!(w, fill("", 1_000_000))
        close(w)
        seekstart(io)
        got = readall(io)
        @test length(got) == 1 && length(got[1]) == 1_000_000 && all(isempty, got[1])
        w2 = Avro.Writer(IOBuffer(), arr)
        @test_throws Avro.LimitError push!(w2, fill("", 4_000_000))                               # ≈ 96 MiB of reader-side slots > 64 MiB
        close(w2; abort=true)
        raised = Avro.Limits(max_block_output_bytes=128 << 20)
        io3 = IOBuffer()
        w3 = Avro.Writer(io3, arr; limits=raised)
        push!(w3, fill("", 4_000_000))
        close(w3)
        seekstart(io3)
        @test_throws Avro.LimitError readall(io3)                                                 # and the default reader refuses it
        seekstart(io3)
        @test length(Avro.Reader(r -> collect(Avro.eachdatum(r)), io3; limits=raised)[1]) == 4_000_000
    end
    @testset "high-window codec bombs and streaming past the ceiling" begin
        hw = joinpath(gen, "highwindow")
        for name in ("zstd-window1g.avro", "xz-dict1g.avro")
            e = try; readall(joinpath(hw, name)); nothing; catch err; err; end
            @test e isa Avro.CodecError                                                           # the member requirement exceeds max_codec_memory
        end
        @test !isempty(readall(joinpath(hw, "zstd-default.avro")))
        @test !isempty(readall(joinpath(hw, "xz-default.avro")))
        if get(ENV, "AVRO_BIG_MEMORY", "true") == "true"
            raised = Avro.Limits(max_codec_memory=2 << 30, max_total_bytes=16 << 30, max_block_bytes=1 << 30, max_block_output_bytes=8 << 30, max_bytes=1 << 30, max_datum_bytes=1 << 30)
            @test !isempty(readall(joinpath(hw, "zstd-window1g.avro"); limits=raised))
            @test !isempty(readall(joinpath(hw, "xz-dict1g.avro"); limits=raised))
        end
        # mmap=false streams a file larger than the effective ceiling (176 MiB here) with one block resident
        small = Avro.Limits(max_total_bytes=176 << 20, max_block_bytes=1 << 20, max_codec_memory=16 << 20, max_block_output_bytes=8 << 20,
                            max_bytes=4 << 20, max_datum_bytes=4 << 20)
        dir = mktempdir()
        big = joinpath(dir, "big.avro")
        bs = P("\"bytes\"")
        chunk = fill(0x7a, 512 << 10)
        w = Avro.Writer(big, bs; limits=small)
        for _ in 1:400
            push!(w, chunk)
        end
        close(w)
        @test filesize(big) > 200 << 20
        n = Avro.Reader(big; limits=small, mmap=false) do r
            total = 0
            for (count, bytes) in Avro.eachblock(r)
                total += count
            end
            total
        end
        @test n == 400
        rm(dir; recursive=true, force=true)
    end
    @testset "inspect" begin
        rows = [(x=Int64(i),) for i in 1:10]
        io = Avro.tobuffer(rows; codec=:deflate, block_bytes=8)
        rep = Avro.inspect(io)
        @test isempty(rep.issues) && rep.codec == "deflate" && rep.datums == 10 && rep.blocks >= 2 && rep.schema isa Avro.RecordSchema
        legrep = Avro.inspect(joinpath(gen, "legacy1x", "avrojl112-zstd.avro"))
        @test any(occursin("zstd", i) for i in legrep.issues)
        buf = take!(Avro.tobuffer(rows))
        trep = Avro.inspect(buf[1:end - 3])
        @test !isempty(trep.issues)
        @test occursin("issues", sprint(show, MIME("text/plain"), trep))
    end
end
