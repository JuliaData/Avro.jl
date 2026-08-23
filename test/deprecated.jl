@testset "1.x deprecation shims (plan §7)" begin
    @test Avro.read !== Base.read

    leg = joinpath(@__DIR__, "fixtures", "generated", "legacy1x", "avrojl112-null.avro")
    t = @test_deprecated Avro.readtable(leg; decimal_byteorder=:little)   # a real 1.1.2 file
    @test t isa Avro.Table && length(t) == 2
    @test Tables.getcolumn(t, :a) == [1, 2] && Tables.getcolumn(t, :b) == ["x", "yy"]
    @test Tables.getcolumn(t, :dec) == [Avro.Decimal(12345, 2), Avro.Decimal(-123, 2)]
    io = IOBuffer()
    @test_deprecated Avro.writetable(io, [(x=Int64(i),) for i in 1:5]; compress=:zstd)   # :zstd maps
    seekstart(io)
    Avro.Reader(io) do r
        @test Avro.codec(r) === :zstandard
    end
    seekstart(io)
    t2 = @test_deprecated Avro.readtable(io)
    @test Tables.getcolumn(t2, :x) == 1:5                                  # shim round trip
    io2 = IOBuffer()
    @test_deprecated Avro.writetable(io2, [(x=Int64(1),)])                 # no compression → null
    seekstart(io2)
    Avro.Reader(io2) do r
        @test Avro.codec(r) === :null
    end

    bytes = @test_deprecated Avro.write(Int64(-4))
    @test bytes == Avro.encode(Int64(-4))
    @test_deprecated Avro.read(bytes, Int64) === Int64(-4)
    datum_schema = Avro.parseschema("\"long\"")
    schema_bytes = @test_deprecated Avro.write(Int32(7); schema=datum_schema)
    @test_deprecated Avro.read(IOBuffer(schema_bytes), datum_schema) === Int64(7)
    union_type = Union{Int64,String}
    union_bytes = @test_deprecated Avro.write("legacy"; schema=union_type)
    @test_deprecated Avro.read(union_bytes, union_type) == "legacy"
    e = try
        Avro.write(IOBuffer(), Int64(1))
        nothing
    catch err
        err
    end
    @test e isa ArgumentError
    @test occursin("Avro.encode!", sprint(showerror, e))

    partitioned = Tables.partitioner([[(x=Int64(1),)], [(x=Int64(2),)]])
    partitioned_io = Avro.tobuffer(partitioned)
    @test Tables.getcolumn(Avro.Table(partitioned_io), :x) == [1, 2]

    streamed = joinpath(@__DIR__, "fixtures", "generated", "roots", "long-fastavro-null.avro")
    Avro.Reader(streamed; mmap=false) do reader
        @test Avro.codec(reader) === :null
        @test !isempty(collect(Avro.eachdatum(reader)))
    end
end
