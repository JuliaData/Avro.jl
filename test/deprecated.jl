@testset "1.x deprecation shims (plan §7)" begin
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
end
