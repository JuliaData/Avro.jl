# Cross-package smoke (plan §6, Phase 5): CSV → Avro → Arrow with the registered Arrow 2.x, and a
# DataFrames round trip through a stored schema. Opt-in: AVRO_SMOKE=true.

import CSV, Arrow, DataFrames
using DataFrames: DataFrame

@testset "Cross-package smoke: CSV → Avro → Arrow, DataFrames" begin
    df = DataFrame(id=Int64.(1:1000), name=["n$i" for i in 1:1000], score=[i / 7 for i in 1:1000],
                   day=[Date(2026, 1, 1) + Day(i % 300) for i in 1:1000])
    dir = mktempdir()
    csvpath = joinpath(dir, "t.csv")
    CSV.write(csvpath, df)
    avropath = joinpath(dir, "t.avro")
    Avro.write(avropath, CSV.File(csvpath); codec=:zstandard)
    t = Avro.Table(avropath)
    @test length(t) == 1000
    arrowpath = joinpath(dir, "t.arrow")
    Arrow.write(arrowpath, t)
    back = DataFrame(Arrow.Table(arrowpath))
    @test back.id == df.id && back.name == df.name && back.score == df.score && back.day == df.day
    # DataFrame → Avro → DataFrame with the schema stored in the container
    avro2 = joinpath(dir, "t2.avro")
    Avro.write(avro2, df)
    df2 = DataFrame(Avro.Table(avro2))
    @test df2.id == df.id && df2.name == df.name && df2.score == df.score && df2.day == df.day
    # the derived schema round-trips as the file's writer schema
    Avro.Reader(avro2) do r
        @test Avro.writerschema(r) isa Avro.RecordSchema
        @test [f.name for f in Avro.writerschema(r).fields] == ["id", "name", "score", "day"]
    end
end
