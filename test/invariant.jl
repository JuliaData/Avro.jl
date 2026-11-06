# Everything a successfully closed Writer emits is accepted by the Reader, Rows and Table under the same
# Limits: across root kinds, codecs and source modes, and at the value cap where writers split blocks.
@testset "Writer/Reader invariant: consumers and source modes" begin
    P = Avro.parseschema
    dir = mktempdir()
    function blockcount(src)
        return Avro.Reader(r -> length(collect(Avro.eachblock(r))), src)
    end
    @testset "recursive field lookup is reentrant" begin
        recursive = P("{\"type\":\"record\",\"name\":\"FieldCache\",\"fields\":[{\"name\":\"child\",\"type\":[\"null\",\"FieldCache\"]},{\"name\":\"tail\",\"type\":\"int\"}]}")
        value = (child=(tail=Int32(2), child=missing), tail=Int32(1))
        @test isequal(Avro.decode(recursive, Avro.encode(recursive, value)), Avro.Record(recursive, Any[
            Avro.Record(recursive, Any[missing, Int32(2)]), Int32(1)]))
    end
    @testset "writers split blocks at the value cap every consumer enforces" begin
        lim = Avro.Limits(max_values_per_byte=1, value_allowance=100)     # 100 values per zero-byte block
        nulls = Avro.NullSchema()
        empty_schema = P("{\"type\":\"record\",\"name\":\"Once\",\"fields\":[]}")
        twonulls = P("{\"type\":\"record\",\"name\":\"TwoNulls\",\"fields\":[{\"name\":\"x\",\"type\":\"null\"},{\"name\":\"y\",\"type\":\"null\"}]}")
        for (schema, value, perdatum) in ((nulls, missing, 1), (empty_schema, (;), 1), (twonulls, (x=missing, y=missing), 3)),
            n in (60, 250)
            io = IOBuffer()
            writer = Avro.Writer(io, schema; limits=lim)
            for _ in 1:n
                push!(writer, value)
            end
            close(writer)
            bytes = take!(io)
            @test blockcount(bytes) >= cld(n * perdatum, 100)
            @test Avro.Reader(r -> length(collect(Avro.eachdatum(r))), IOBuffer(bytes); limits=lim) == n
            rows = Avro.Rows(IOBuffer(bytes); limits=lim)
            @test length(collect(rows)) == n
            close(rows)
            if schema isa Avro.RecordSchema
                @test length(Avro.Table(IOBuffer(bytes); limits=lim, ntasks=1)) == n
                @test length(Avro.Table(bytes; limits=lim, ntasks=2)) == n
            end
        end
    end
    @testset "writer and reader count values alike" begin
        schema = Avro.StringSchema()
        for codec in Avro.codecs()
            io = IOBuffer()
            writer = Avro.Writer(io, schema; codec=codec)
            push!(writer, "x")
            close(writer)
            @test Avro.Reader(r -> collect(Avro.eachdatum(r)), take!(io)) == ["x"]
        end
        # a datum over its own value cap is refused before anything reaches the sink
        tight = Avro.Limits(max_values_per_byte=1, value_allowance=0)
        tightio = IOBuffer()
        tightwriter = Avro.Writer(tightio, Avro.ArraySchema(Avro.NullSchema()); limits=tight)
        headersize = position(tightio)
        err = try
            push!(tightwriter, fill(missing, 10))                    # 11 values in 2 bytes
            close(tightwriter)
            nothing
        catch e
            close(tightwriter; abort=true)
            e
        end
        @test err isa Avro.LimitError && err.limit === :max_values_per_byte
        @test position(tightio) == headersize                         # no unreadable data block was emitted
        # aligned NamedTuple rows: the second row would push the block over the cap, so it starts the next block
        recordschema = P("""{"type":"record","name":"AlignedWork","fields":[
            {"name":"id","type":"long"},{"name":"gap","type":"null"},{"name":"name","type":"string"}]}""")
        recordlimits = Avro.Limits(max_values_per_byte=1, value_allowance=2)   # one 4-value, 2-byte row per block
        recordrows = [(id=Int64(1), gap=missing, name=""), (id=Int64(2), gap=missing, name="")]
        recordio = IOBuffer()
        recordwriter = Avro.Writer(recordio, recordschema; limits=recordlimits)
        for row in recordrows
            push!(recordwriter, row)
        end
        close(recordwriter)
        alignedbytes = take!(recordio)
        @test blockcount(alignedbytes) == 2
        recordvalues = Avro.Reader(r -> collect(Avro.eachdatum(r)), alignedbytes; limits=recordlimits)
        @test getproperty.(recordvalues, :id) == Int64[1, 2]
        @test getproperty.(recordvalues, :name) == ["", ""]
        @test Tables.getcolumn(Avro.Table(alignedbytes; limits=recordlimits), :id) == Int64[1, 2]
    end
    @testset "a datum over its own value cap is refused by the writer" begin
        dense = fill(nothing, 67_000)                                  # 67,001 values in four bytes
        dense_writer = Avro.Writer(IOBuffer(), Avro.ArraySchema(Avro.NullSchema()))
        err = try
            push!(dense_writer, dense)
            close(dense_writer)
            nothing
        catch e
            e
        end
        err === nothing || close(dense_writer; abort=true)
        @test err isa Avro.LimitError && err.limit === :max_values_per_byte
    end
    @testset "a large record file through every source mode and consumer" begin
        lim = Avro.Limits(max_bytes=1 << 20)
        s = P("{\"type\":\"record\",\"name\":\"N\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"string\"},{\"name\":\"c\",\"type\":[\"null\",\"double\"]}]}")
        n = 260_000
        rows = [(a=Int64(i), b="value-$(i)-payloadpayloadpayloadpayloadpayload", c=iseven(i) ? i / 3 : missing) for i in 1:n]
        path = joinpath(dir, "near.avro")
        Avro.write(path, rows; schema=s, limits=lim, codec=:deflate)
        bytes = read(path)
        for mode in (:bytes, :mapped, :streampath, :io)
            function src()
                return mode === :bytes ? IOBuffer(bytes) : mode === :io ? open(path) : path
            end
            kw = mode === :streampath ? (; mmap=false) : (;)
            cnt = Avro.Reader(rr -> sum(first(b) for b in Avro.eachblock(rr); init=0), src(); limits=lim, kw...)
            @test cnt == n
            rl = Avro.Rows(src(); limits=lim, kw...)
            total = 0
            last = nothing
            for row in rl
                total += 1
                last = row
            end
            close(rl)
            @test total == n && last.a === Int64(n)
            t = Avro.Table(src(); limits=lim, kw...)
            @test length(t) == n && Tables.getcolumn(t, :a)[end] == n && Tables.getcolumn(t, :b)[1] == rows[1].b
            @test ismissing(Tables.getcolumn(t, :c)[1]) && Tables.getcolumn(t, :c)[2] == 2 / 3
        end
    end
    @testset "root shapes and codecs through every consumer under the same limits" begin
        lim = Avro.Limits(max_values_per_byte=1, value_allowance=100)
        shapes = [
            ("\"null\"", fill(nothing, 1000), false),
            ("{\"type\":\"record\",\"name\":\"E\",\"fields\":[]}", fill((;), 1000), true),
            ("{\"type\":\"record\",\"name\":\"AN\",\"fields\":[{\"name\":\"x\",\"type\":\"null\"},{\"name\":\"y\",\"type\":\"null\"}]}",
             fill((x=nothing, y=nothing), 1000), true),
            ("{\"type\":\"fixed\",\"name\":\"F0\",\"size\":0}", fill(UInt8[], 1000), false),
            ("{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":\"long\"}}", fill(Vector{Int64}[], 1000), false),
            ("{\"type\":\"record\",\"name\":\"NR\",\"fields\":[{\"name\":\"inner\",\"type\":{\"type\":\"record\",\"name\":\"NI\",\"fields\":[{\"name\":\"x\",\"type\":\"null\"}]}}]}",
             fill((inner=(x=nothing,),), 1000), true),
        ]
        for (json, vals, isrecord) in shapes, codec in Avro.codecs()
            s = P(json)
            io = IOBuffer()
            w = Avro.Writer(io, s; block_bytes=256, codec=codec, limits=lim)
            for v in vals
                push!(w, v)
            end
            close(w)
            bytes = take!(io)
            cnt = Avro.Reader(rr -> sum(first(b) for b in Avro.eachblock(rr); init=0), IOBuffer(bytes); limits=lim)
            @test cnt == 1000
            @test length(Avro.Reader(rr -> collect(Avro.eachdatum(rr)), IOBuffer(bytes); limits=lim)) == 1000
            rl = Avro.Rows(IOBuffer(bytes); limits=lim)
            @test count(Returns(true), rl) == 1000
            close(rl)
            if isrecord
                t = Avro.Table(IOBuffer(bytes); limits=lim)
                @test length(t) == 1000
            else
                @test_throws Avro.SchemaError Avro.Table(IOBuffer(bytes); limits=lim)
            end
        end
    end
    @testset "ordinary tables write and read under default limits" begin
        # A 4-column Int64 table of 4.4M rows (a 17 MiB file, 141 MB of columns) under default limits.
        n = 4_400_000
        cols = (a=collect(1:n), b=collect(n:-1:1), c=fill(7, n), d=collect(1:n) .% 97)
        path = joinpath(dir, "ordinary.avro")
        Avro.write(path, cols)
        t = Avro.Table(path)
        @test length(t) == n && Tables.getcolumn(t, :b)[1] == n && Tables.getcolumn(t, :d)[end] == n % 97
        @test Avro.Rows(rr -> count(Returns(true), rr), path) == n
        # one-row blocks of a wide mostly-null record: every consumer accepts what the writer wrote
        fields = join(("{\"name\":\"f$i\",\"type\":[\"null\",\"long\"]}" for i in 1:1001), ",")
        ws = P("{\"type\":\"record\",\"name\":\"W\",\"fields\":[$fields]}")
        row = Dict{String,Any}("f$i" => nothing for i in 1:1001)
        wide = joinpath(dir, "wide.avro")
        w = Avro.Writer(wide, ws; block_bytes=1)                 # every datum flushes: one-row blocks
        for _ in 1:10_000
            push!(w, row)
        end
        close(w)
        for t in (Avro.Table(open(wide)), Avro.Table(wide))      # streamed and mapped consumers
            @test length(t) == 10_000 && length(Tables.columnnames(t)) == 1001
            @test all(ismissing, Tables.getcolumn(t, :f1))
        end
        @test Avro.Rows(rr -> count(Returns(true), rr), wide) == 10_000
        @test Avro.Reader(rr -> sum(first(b) for b in Avro.eachblock(rr); init=0), wide) == 10_000
    end
    @testset "claimed block counts cannot drive the Table preallocation" begin
        # 50 two-byte blocks claim 2^24 rows each: preallocating 2^24 x 50 rows would commit gigabytes
        # before the first block is decoded, so such claims decode block by block and fail there.
        schema = """{"type":"record","name":"Claim","fields":[{"name":"a","type":"long"},{"name":"s","type":"string"}]}"""
        long = Avro.LongSchema()
        io = IOBuffer()
        write(io, b"Obj\x01", Avro.encode(long, Int64(1)), Avro.encode(long, Int64(11)), "avro.schema",
              Avro.encode(long, Int64(sizeof(schema))), schema, Avro.encode(long, Int64(0)))
        sync = zeros(UInt8, 16)
        write(io, sync)
        for _ in 1:50
            write(io, Avro.encode(long, Int64(1) << 24), Avro.encode(long, Int64(2)), UInt8[0, 0], sync)
        end
        claims = take!(io)
        @test_throws Union{Avro.DataError,Avro.LimitError} Avro.Table(claims)
        @test_throws Union{Avro.DataError,Avro.LimitError} Avro.Table(claims; ntasks=1)
        @test @allocated(try Avro.Table(claims) catch end) < 64 << 20
        @test length(Avro.Table(take!(Avro.tobuffer((a=collect(1:1000), s=fill("x", 1000)))))) == 1000
    end
end
