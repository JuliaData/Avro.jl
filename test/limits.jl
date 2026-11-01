@testset "Limits" begin
    P = Avro.parseschema
    @testset "keywords, defaults, validation and show" begin
        l = Avro.Limits()
        @test l === Avro.Limits()
        @test fieldnames(Avro.Limits) == (:max_depth, :max_bytes, :max_values_per_byte, :value_allowance)
        @test (l.max_depth, l.max_bytes, l.max_values_per_byte, l.value_allowance) == (1024, 256 << 20, 16, 65_536)
        custom = Avro.Limits(max_depth=8)
        @test custom.max_depth == 8 && custom.max_bytes == l.max_bytes && custom.value_allowance == l.value_allowance
        @test Avro.Limits(max_bytes=0, value_allowance=0).max_bytes == 0
        @test Avro.Limits(max_bytes=typemax(Int)).max_bytes == typemax(Int)
        @test Avro.Limits(max_depth=1024) == l
        for bad in ((bogus=1,), (max_depth=-1,), (max_depth=0,), (max_values_per_byte=0,),
                    (max_bytes=-1,), (value_allowance=-1,), (max_bytes=1.5,), (max_bytes=true,),
                    (max_bytes=big(typemax(Int)) + 1,))
            @test_throws ArgumentError Avro.Limits(; bad...)
        end
        @test sprint(show, l) == "Avro.Limits()"
        @test sprint(show, MIME("text/plain"), l) == "Avro.Limits()"
        @test sprint(show, Avro.Limits(max_depth=8, value_allowance=0)) == "Avro.Limits(max_depth=8, value_allowance=0)"
    end

    @testset "LimitError names the limit and the keyword that raises it" begin
        e = Avro.LimitError(:max_depth, 1025, 1024)
        @test fieldnames(Avro.LimitError) == (:limit, :observed, :value)
        @test (e.limit, e.observed, e.value) == (:max_depth, 1025, 1024)
        @test sprint(showerror, e) == "LimitError: max_depth exceeded: observed 1025, limit 1024; raise `Avro.Limits(max_depth=...)`"
    end

    @testset "max_depth: nested data and schema JSON" begin
        node = P("""{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]}]}""")
        chain(depth) = vcat(fill(0x02, depth - 1), 0x00)            # `depth` nested records
        @test Avro.decode(node, chain(8); limits=Avro.Limits(max_depth=8)) isa Avro.Record
        err = try
            Avro.decode(node, chain(9); limits=Avro.Limits(max_depth=8))
            nothing
        catch caught
            caught
        end
        @test err isa Avro.LimitError && err.limit === :max_depth && err.observed == 9 && err.value == 8
        @test_throws Avro.LimitError Avro.decode(node, chain(2000))                     # the default of 1024
        deepvalue = Avro.Record(node, Any[missing])
        for _ in 1:1100
            deepvalue = Avro.Record(node, Any[deepvalue])
        end
        @test_throws Avro.LimitError Avro.encode(node, deepvalue)
        nestedjson(depth) = "{\"type\":\"array\",\"items\":"^depth * "\"int\"" * "}"^depth
        @test P(nestedjson(7); limits=Avro.Limits(max_depth=8)) isa Avro.ArraySchema
        jsonerr = try
            P(nestedjson(20); limits=Avro.Limits(max_depth=8))
            nothing
        catch caught
            caught
        end
        @test jsonerr isa Avro.LimitError && jsonerr.limit === :max_depth && jsonerr.value == 8
        @test_throws Avro.LimitError P(nestedjson(2000))
        @test_throws Avro.LimitError Avro.fromjson(node, "{\"next\":{\"Node\":"^20 * "null" * "}}"^20;
                                                   limits=Avro.Limits(max_depth=8))
    end

    @testset "max_bytes: container blocks, schema documents, stream datums" begin
        # a decompressed container block
        compressible = IOBuffer()
        Avro.Writer(w -> push!(w, "x"^100_000), compressible, Avro.StringSchema(); codec=:deflate)
        blockfile = take!(compressible)
        @test only(Avro.Reader(r -> collect(Avro.eachdatum(r)), blockfile)) == "x"^100_000
        blockerr = try
            Avro.Reader(r -> collect(Avro.eachdatum(r)), blockfile; limits=Avro.Limits(max_bytes=64 << 10))
            nothing
        catch caught
            caught
        end
        @test blockerr isa Avro.LimitError && blockerr.limit === :max_bytes && blockerr.value == 64 << 10
        # a schema document, from text and from a stream
        doc = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[" * join(("{\"name\":\"f$i\",\"type\":\"int\"}" for i in 1:100), ",") * "]}"
        @test P(doc; limits=Avro.Limits(max_bytes=sizeof(doc))) isa Avro.RecordSchema
        @test P(IOBuffer(doc); limits=Avro.Limits(max_bytes=sizeof(doc))) isa Avro.RecordSchema
        docerr = try
            P(doc; limits=Avro.Limits(max_bytes=sizeof(doc) - 1))
            nothing
        catch caught
            caught
        end
        @test docerr isa Avro.LimitError && docerr.limit === :max_bytes && docerr.value == sizeof(doc) - 1
        @test_throws Avro.LimitError P(IOBuffer(doc); limits=Avro.Limits(max_bytes=sizeof(doc) - 1))
        # a container header's schema metadata
        @test_throws Avro.LimitError Avro.Reader(take!(Avro.tobuffer([(a=1,)])); limits=Avro.Limits(max_bytes=16))
        # one datum read from a stream
        datum = Avro.encode(Avro.StringSchema(), "x"^100)              # 102 bytes: a 2-byte length varint
        @test Avro.decode(Avro.StringSchema(), IOBuffer(datum); limits=Avro.Limits(max_bytes=102)) == "x"^100
        streamerr = try
            Avro.decode(Avro.StringSchema(), IOBuffer(datum); limits=Avro.Limits(max_bytes=101))
            nothing
        catch caught
            caught
        end
        @test streamerr isa Avro.LimitError && streamerr.limit === :max_bytes && streamerr.value == 101
        @test Avro.decode(Avro.StringSchema(), datum; limits=Avro.Limits(max_bytes=100)) == "x"^100   # bytes in memory are not read from a stream
    end

    @testset "value density: per standalone datum and per container block" begin
        nulls = Avro.ArraySchema(Avro.NullSchema())
        million = vcat(Avro.encode(Avro.LongSchema(), 1_000_000), UInt8[0x00])     # a million nulls in four bytes
        @test length(million) == 4
        err = try
            Avro.decode(nulls, million)
            nothing
        catch caught
            caught
        end
        @test err isa Avro.LimitError && err.limit === :max_values_per_byte
        @test err.value == 16 * length(million) + 65_536 && err.observed > err.value
        @test occursin("raise `Avro.Limits(max_values_per_byte=...)`", sprint(showerror, err))
        for raised in (Avro.Limits(value_allowance=1_000_001), Avro.Limits(max_values_per_byte=1_000_000))
            v = Avro.decode(nulls, million; limits=raised)
            @test length(v) == 1_000_000 && all(ismissing, v)
        end
        exact = 1_000_001 - 16 * length(million)                            # the array value and its nulls
        @test length(Avro.decode(nulls, million; limits=Avro.Limits(value_allowance=exact))) == 1_000_000
        @test_throws Avro.LimitError Avro.decode(nulls, million; limits=Avro.Limits(value_allowance=exact - 1))
        @test_throws Avro.LimitError Avro.DatumReader(nulls)(million)
        @test length(Avro.DatumReader(nulls; limits=Avro.Limits(value_allowance=exact))(million)) == 1_000_000
        # encoding is not capped; the cap applies where untrusted bytes are read
        encoded = Avro.encode(nulls, fill(missing, 1_000_000))
        @test_throws Avro.LimitError Avro.decode(nulls, encoded)
        @test length(Avro.decode(nulls, encoded; limits=Avro.Limits(value_allowance=2_000_000))) == 1_000_000
        # each standalone datum has its own allowance
        sixty = vcat(Avro.encode(Avro.LongSchema(), 60_000), UInt8[0x00])
        @test all(length(Avro.decode(nulls, sixty)) == 60_000 for _ in 1:3)
        threeinone = vcat(sixty, sixty, sixty)
        pos = 1
        for _ in 1:3
            value, pos = Avro.decode(nulls, threeinone, pos)
            @test length(value) == 60_000
        end
        @test pos == length(threeinone) + 1
        # each container block has its own allowance: the writer splits zero-byte datums at the cap
        n = 200_000
        nullio = IOBuffer()
        nullwriter = Avro.Writer(nullio, Avro.NullSchema())
        for _ in 1:n
            push!(nullwriter, missing)
        end
        close(nullwriter)
        nullfile = take!(nullio)
        @test Avro.Reader(r -> length(collect(Avro.eachblock(r))), nullfile) >= cld(n, 65_536)
        @test Avro.Reader(r -> count(Returns(true), Avro.eachdatum(r)), nullfile) == n
        rows = Avro.Rows(nullfile)
        @test count(Returns(true), rows) == n
        close(rows)
        # one block of those datums, written under a raised allowance, is over the default cap
        raised = Avro.Limits(value_allowance=1_000_000)
        bigio = IOBuffer()
        bigwriter = Avro.Writer(bigio, Avro.NullSchema(); limits=raised)
        for _ in 1:n
            push!(bigwriter, missing)
        end
        close(bigwriter)
        bigfile = take!(bigio)
        @test Avro.Reader(r -> length(collect(Avro.eachblock(r))), bigfile) == 1
        blockerr = try
            Avro.Reader(r -> count(Returns(true), Avro.eachdatum(r)), bigfile)
            nothing
        catch caught
            caught
        end
        @test blockerr isa Avro.LimitError && blockerr.limit === :max_values_per_byte && blockerr.value == 65_536
        @test Avro.Reader(r -> count(Returns(true), Avro.eachdatum(r)), bigfile; limits=raised) == n
    end

    @testset "claimed lengths are checked against the remaining bytes before allocation" begin
        longs = P("{\"type\":\"array\",\"items\":\"long\"}")
        @test_throws Avro.DataError Avro.decode(Avro.StringSchema(), UInt8[0x04, 0x61])   # 2 bytes claimed, 1 remains
        @test_throws Avro.DataError Avro.decode(longs, hex2bytes("d00f00"))              # 1000 longs claimed, 1 byte remains
        @test_throws Avro.DataError Avro.decode(longs, hex2bytes("0306"))                # a sized block beyond the data
        huge = vcat(Avro.encode(Avro.LongSchema(), typemax(Int64)), UInt8[0x00])
        for schema in (Avro.StringSchema(), Avro.BytesSchema(), longs, P("{\"type\":\"map\",\"values\":\"long\"}"))
            @test_throws Avro.DataError Avro.decode(schema, huge)
            @test @allocated(try Avro.decode(schema, huge) catch end) < 1 << 20
        end
        @test_throws Avro.DataError Avro.decode(Avro.FixedSchema("Huge", typemax(Int) - 1), UInt8[0x00])
    end

    @testset "a prepared reader recovers after a failed call" begin
        schema = P("""{"type":"record","name":"Guarded","fields":[{"name":"payload","type":"bytes"},{"name":"valid","type":"boolean"}]}""")
        good = Avro.encode(schema, (payload=zeros(UInt8, 1 << 20), valid=true))
        bad = copy(good)
        bad[end] = 0x02
        reader = Avro.DatumReader(schema)
        @test_throws Avro.DataError reader(bad)
        @test reader(good).valid === true
    end

    @testset "saturated limits do not overflow" begin
        saturated = Avro.Limits(max_bytes=typemax(Int), max_values_per_byte=typemax(Int), value_allowance=typemax(Int))
        ints = Avro.IntSchema()
        @test Avro.decode(ints, Avro.encode(ints, Int32(7)); limits=saturated) == Int32(7)
        @test Avro.fromjson(ints, "7"; limits=saturated) == Int32(7)
        @test P("{\"type\":\"record\",\"name\":\"Saturated\",\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}";
                limits=saturated) isa Avro.RecordSchema
        saturatedio = IOBuffer()
        saturatedwriter = Avro.Writer(saturatedio, ints; limits=saturated)
        push!(saturatedwriter, Int32(7))
        close(saturatedwriter)
        @test Avro.Reader(reader -> collect(Avro.eachdatum(reader)), take!(saturatedio);
                          limits=saturated) == Any[Int32(7)]
    end

    @testset "error types" begin
        @test sprint(showerror, Avro.SchemaError("bad", "\$.fields[0]")) == "SchemaError: bad (at \$.fields[0])"
        @test sprint(showerror, Avro.DataError("truncated", 7)) == "DataError: truncated (at byte 7)"
        @test sprint(showerror, Avro.UnsupportedCodecError("bzip2", "CodecBzip2")) == "UnsupportedCodecError: codec \"bzip2\" requires `using CodecBzip2`"
        @test sprint(showerror, Avro.UnknownSchemaError(UInt64(255))) == "UnknownSchemaError: no schema registered for fingerprint 0x00000000000000ff"
        @test occursin("poisoned", sprint(showerror, Avro.WriterClosedError(ErrorException("x"))))
        @test Avro.DataError <: Avro.DecodeError <: Avro.AvroError
        for T in (Avro.SchemaError, Avro.EncodeError, Avro.ResolutionError, Avro.LimitError, Avro.CodecError,
                  Avro.UnsupportedCodecError, Avro.ConversionError, Avro.UnknownSchemaError,
                  Avro.AmbiguousSchemaError, Avro.WriterClosedError)
            @test T <: Avro.AvroError
        end
    end
end
