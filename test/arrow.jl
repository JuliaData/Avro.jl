# Arrow.jl interop. Opt-in: AVRO_ARROW=true, in an environment with Arrow.jl, which the test project leaves out.
# The file also runs on its own: `julia --project=env test/arrow.jl`.

using Test, Avro, Dates, Tables, UUIDs
import Arrow, CSV, DataFrames

@testset "Arrow.jl" begin
    P = Avro.parseschema

    @testset "Arrow preserves exact Avro timestamps" begin
        for (period, unit) in ((Millisecond, Arrow.Meta.TimeUnit.MILLISECOND),
                               (Microsecond, Arrow.Meta.TimeUnit.MICROSECOND),
                               (Nanosecond, Arrow.Meta.TimeUnit.NANOSECOND))
            for (make, zone) in ((Avro.timestamp, :UTC), (Avro.localtimestamp, nothing))
                ticks = Int64[typemin(Int64), -1, 0, 1, typemax(Int64)]
                stamps = [make(period, t) for t in ticks]
                T = eltype(stamps)
                for values in (stamps, Union{Missing,T}[missing; stamps])
                    table = Avro.Table(Avro.tobuffer((time=values,)))
                    io = IOBuffer()
                    Arrow.write(io, table)
                    bytes = take!(io)
                    restored = Arrow.Table(bytes)
                    @test eltype(restored.time) === eltype(values)
                    @test isequal(collect(restored.time), values)
                    native = Arrow.Table(bytes; convert=false)
                    @test Base.nonmissingtype(eltype(native.time)) === Arrow.Timestamp{unit,zone}
                    @test [x.x for x in skipmissing(native.time)] == ticks
                    @test isequal(Avro.Table(Avro.tobuffer(restored)).time, values)
                end
            end
        end
    end

    @testset "Arrow writes every value kind" begin
        s = P("""{"type":"record","name":"O","fields":[
            {"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]}},
            {"name":"f","type":{"type":"fixed","name":"F","size":2}},
            {"name":"u","type":["long","string"]},
            {"name":"dur","type":{"type":"fixed","name":"D","size":12,"logicalType":"duration"}},
            {"name":"dec","type":{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}},
            {"name":"m","type":{"type":"map","values":"long"}},
            {"name":"r","type":{"type":"record","name":"I","fields":[
                {"name":"x","type":"int"},{"name":"y","type":["null","string"]},{"name":"e2","type":"E"},{"name":"f2","type":"F"},
                {"name":"z","type":{"type":"array","items":{"type":"record","name":"J","fields":[{"name":"k","type":"long"}]}}}]}},
            {"name":"n","type":["null","I"]}
        ]}""")
        inner(i, y, z) = (x=Int32(i), y=y, e2="B", f2=UInt8[9, 9], z=z)
        rows = [(e="A", f=UInt8[1, 2], u=1, dur=Avro.Duration(UInt32(1), UInt32(2), UInt32(3)), dec=Avro.Decimal(Int128(125), 2), m=Dict("a" => 1),
                 r=inner(1, missing, [(k=1,), (k=2,)]), n=missing),
                (e="B", f=UInt8[3, 4], u="x", dur=Avro.Duration(UInt32(4), UInt32(5), UInt32(6)), dec=Avro.Decimal(Int128(250), 2), m=Dict("b" => 2),
                 r=inner(2, "q", NamedTuple{(:k,),Tuple{Int64}}[]), n=inner(9, "w", [(k=3,)]))]
        t = Avro.Table(Avro.tobuffer(rows; schema=s))
        io = IOBuffer()
        Arrow.write(io, t)
        a = Arrow.Table(take!(io))
        @test a.e == ["A", "B"] && a.f == [UInt8[1, 2], UInt8[3, 4]] && a.u == [1, "x"] && a.m[2]["b"] == 2
        @test a.dur[2] == (months=4, days=5, millis=6)
        @test a.r[1].x == 1 && ismissing(a.r[1].y) && a.r[2].y == "q" && a.r[1].e2 == "B" && a.r[1].f2 == UInt8[9, 9]
        @test [z.k for z in a.r[1].z] == [1, 2] && isempty(a.r[2].z)
        @test ismissing(a.n[1]) && a.n[2].x == 9 && a.n[2].z[1].k == 3
        allmissing = Avro.Table(Avro.tobuffer([(id=1, n=missing)]; schema=P("""{"type":"record","name":"W","fields":[{"name":"id","type":"long"},
            {"name":"n","type":["null",{"type":"record","name":"V","fields":[{"name":"v","type":"long"}]}]}]}""")))
        io = IOBuffer()
        Arrow.write(io, allmissing)
        @test Arrow.Table(take!(io)).id == [1]
        # value types that differ between blocks still form one Arrow column, from a Table or from Rows
        blocks = take!(Avro.tobuffer(Tables.partitioner([rows[1:1], rows[2:2]]); schema=s))
        @test length(Avro.Reader(r -> collect(Avro.eachblock(r)), blocks)) == 2
        for source in (Avro.Table(blocks), Avro.Rows(blocks))
            io = IOBuffer()
            Arrow.write(io, source)
            bytes = take!(io)
            batched = Arrow.Table(bytes)
            @test length(collect(Arrow.Stream(bytes))) == 1
            @test batched.u == [1, "x"] && batched.r[2].y == "q" && ismissing(batched.r[1].y) && ismissing(batched.n[1]) && batched.n[2].x == 9
        end
        # rows whose columns have fixed types stream one batch per block
        flat = take!(Avro.tobuffer(Tables.partitioner([[(a=1, b=missing)], [(a=2, b="x")]]); schema=P("""{"type":"record","name":"F","fields":[{"name":"a","type":"long"},{"name":"b","type":["null","string"]}]}""")))
        io = IOBuffer()
        Arrow.write(io, Avro.Rows(flat))
        bytes = take!(io)
        @test length(collect(Arrow.Stream(bytes))) == 2 && isequal(Arrow.Table(bytes).b, [missing, "x"])
        empty = Avro.Table(Avro.tobuffer(rows[1:0]; schema=s))
        io = IOBuffer()
        Arrow.write(io, empty)
        @test length(Arrow.Table(take!(io)).e) == 0
        # each kind nested in a record, where a missing parent makes Arrow store a placeholder
        id = uuid4()
        for (field, value, stored) in (("{\"type\":\"string\",\"logicalType\":\"uuid\"}", id, string(id)),
                                       ("{\"type\":\"fixed\",\"name\":\"U\",\"size\":16,\"logicalType\":\"uuid\"}", id, string(id)),
                                       ("{\"type\":\"int\",\"logicalType\":\"date\"}", Date(2024), Date(2024)),
                                       ("{\"type\":\"long\",\"logicalType\":\"time-micros\"}", Time(1), Time(1)),
                                       ("{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}", DateTime(2024), Avro.timestamp(Microsecond, DateTime(2024))),
                                       ("{\"type\":\"long\",\"logicalType\":\"local-timestamp-micros\"}", DateTime(2024), Avro.localtimestamp(Microsecond, DateTime(2024))),
                                       ("{\"type\":\"map\",\"values\":{\"type\":\"map\",\"values\":\"long\"}}", Dict("a" => Dict("b" => 1)), Dict("a" => Dict("b" => 1))),
                                       ("[\"long\",\"string\"]", "x", "x"))
            nullable = P("""{"type":"record","name":"W","fields":[{"name":"p","type":["null",{"type":"record","name":"V","fields":[{"name":"f","type":$field}]}]}]}""")
            io = IOBuffer()
            Arrow.write(io, Avro.Table(Avro.tobuffer([(p=missing,), (p=(f=value,),)]; schema=nullable)))
            nested = Arrow.Table(take!(io))
            @test ismissing(nested.p[1]) && nested.p[2].f == stored
        end
        recursive = P("""{"type":"record","name":"L","fields":[{"name":"n","type":["null","L"]},{"name":"v","type":"long"}]}""")
        err = try
            Arrow.write(IOBuffer(), Avro.Table(Avro.tobuffer([(n=(n=missing, v=2), v=1)]; schema=recursive)))
            nothing
        catch caught
            caught
        end
        @test sprint(showerror, err) isa String && occursin("recursive record L", sprint(showerror, err))
        io = IOBuffer()
        Arrow.write(io, (a=[1, 2], b=["x", missing], c=[[1], [2, 3]], d=[(p=1, q="z"), (p=2, q="w")]))
        back = Avro.Table(Avro.tobuffer(Arrow.Table(take!(io))))
        @test back.a == [1, 2] && isequal(back.b, ["x", missing]) && back.c == [[1], [2, 3]] && back.d[2].q == "w"
    end

    @testset "Arrow sources and sinks" begin
        # a source of partitions that is not itself a table
        io = IOBuffer()
        Arrow.write(io, Tables.partitioner([(a=[1, 2],), (a=[3],)]))
        @test Avro.Table(Avro.tobuffer(Arrow.Stream(take!(io)))).a == [1, 2, 3]
        # an empty file still has columns
        empty = take!(Avro.tobuffer(NamedTuple{(:a,),Tuple{Int}}[]))
        io = IOBuffer()
        Arrow.write(io, Avro.Rows(empty))
        @test collect(Tables.columnnames(Arrow.Table(take!(io)))) == [:a]
    end

    @testset "CSV → Avro → Arrow" begin
        df = DataFrames.DataFrame(id=Int64.(1:1000), name=["n$i" for i in 1:1000], score=[i / 7 for i in 1:1000],
                                  day=[Date(2026, 1, 1) + Day(i % 300) for i in 1:1000])
        dir = mktempdir()
        csvpath = joinpath(dir, "t.csv")
        CSV.write(csvpath, df)
        avropath = joinpath(dir, "t.avro")
        Avro.write(avropath, CSV.File(csvpath); codec=:zstandard)
        arrowpath = joinpath(dir, "t.arrow")
        Arrow.write(arrowpath, Avro.Table(avropath))
        back = DataFrames.DataFrame(Arrow.Table(arrowpath))
        @test back.id == df.id && back.name == df.name && back.score == df.score && back.day == df.day
    end
end
