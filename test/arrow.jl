# Arrow.jl 3 interop. Opt-in: AVRO_ARROW=true, in an environment with Arrow.jl 3, which the test project
# leaves out. The file also runs on its own, as CI's Arrow job runs it: `julia --project=env test/arrow.jl`.

using Test, Avro, Dates, Tables, UUIDs
import Arrow, CSV, DataDecimals, DataFrames, Durations

function arrowbytes(x)
    return take!(Arrow.tobuffer(x))
end

function arrowtable(x)
    return Arrow.Table(arrowbytes(x))
end

# A field of a struct row. Arrow.jl reads a struct that holds an extension type, such as a UUID, as
# `name => value` pairs instead of a NamedTuple.
function structfield(row, name::Symbol)
    return row isa NamedTuple ? getproperty(row, name) : Dict(row)[String(name)]
end

@testset "Arrow.jl" begin
    P = Avro.parseschema

    @testset "timestamps keep their unit and zone" begin
        for period in (Millisecond, Microsecond, Nanosecond), make in (Avro.timestamp, Avro.localtimestamp)
            ticks = Int64[typemin(Int64), -1, 0, 1, typemax(Int64)]
            stamps = [make(period, t) for t in ticks]
            T = eltype(stamps)
            for values in (stamps, Union{Missing,T}[missing; stamps])
                restored = arrowtable(Avro.Table(Avro.tobuffer((time=values,))))
                @test eltype(restored.time) === eltype(values)
                @test isequal(collect(restored.time), values)
                @test [Dates.value(x) for x in skipmissing(restored.time)] == ticks
                @test isequal(Avro.Table(Avro.tobuffer(restored)).time, values)
            end
        end
    end

    @testset "top-level columns" begin
        s = P("""{"type":"record","name":"O","fields":[
            {"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]}},
            {"name":"f","type":{"type":"fixed","name":"F","size":2}},
            {"name":"u","type":["long","string"]},
            {"name":"id","type":{"type":"string","logicalType":"uuid"}},
            {"name":"dur","type":{"type":"fixed","name":"D","size":12,"logicalType":"duration"}},
            {"name":"day","type":{"type":"int","logicalType":"date"}},
            {"name":"m","type":{"type":"map","values":"long"}},
            {"name":"a","type":{"type":"array","items":["null","string"]}}]}""")
        ids = [uuid4(), uuid4()]
        rows = [(e="A", f=UInt8[1, 2], u=1, id=ids[1], dur=Avro.Duration(UInt32(1), UInt32(2), UInt32(3)), day=Date(2024, 1, 2),
                 m=Dict("a" => 1), a=["x", missing]),
                (e="B", f=UInt8[3, 4], u="x", id=ids[2], dur=Avro.Duration(UInt32(4), UInt32(5), UInt32(6)), day=Date(1969, 12, 31),
                 m=Dict{String,Int}(), a=String[])]
        a = arrowtable(Avro.Table(Avro.tobuffer(rows; schema=s)))
        @test a.e == ["A", "B"] && eltype(a.e) <: AbstractString
        @test a.f == [UInt8[1, 2], UInt8[3, 4]]
        @test a.u == [1, "x"]
        @test a.id == ids && eltype(a.id) === UUID
        @test a.dur == [Durations.Duration(1, 2, 3_000_000), Durations.Duration(4, 5, 6_000_000)]
        @test a.day == [Date(2024, 1, 2), Date(1969, 12, 31)]
        @test a.m[1]["a"] == 1 && isempty(a.m[2])
        @test isequal(collect(a.a[1]), ["x", missing]) && isempty(a.a[2])
        # a decimal column takes a precision and scale before it writes as an Arrow decimal
        price = P("""{"type":"record","name":"Q","fields":[{"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}}]}""")
        t = Avro.Table(Avro.tobuffer([(price=Avro.Decimal(Int128(12345), 2),)]; schema=price))
        q = arrowtable((price=DataDecimals.Decimal{9,2}.(t.price),))
        @test eltype(q.price) === DataDecimals.Decimal{9,2,Int128} && q.price == [DataDecimals.Decimal{9,2,Int128}("123.45")]
    end

    @testset "nested records take their schema's struct type" begin
        s = P("""{"type":"record","name":"O","fields":[
            {"name":"r","type":{"type":"record","name":"I","fields":[
                {"name":"x","type":"int"},
                {"name":"y","type":["null","string"]},
                {"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]}},
                {"name":"f","type":{"type":"fixed","name":"F","size":2}},
                {"name":"dec","type":{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}},
                {"name":"wide","type":{"type":"fixed","name":"W","size":20,"logicalType":"decimal","precision":40,"scale":3}},
                {"name":"dur","type":{"type":"fixed","name":"D","size":12,"logicalType":"duration"}},
                {"name":"v","type":["long","string"]},
                {"name":"z","type":{"type":"array","items":{"type":"record","name":"J","fields":[{"name":"k","type":"long"}]}}}]}},
            {"name":"n","type":["null","I"]}]}""")
        inner(i, y, v, z) = (x=Int32(i), y=y, e="B", f=UInt8[9, 9], dec=Avro.Decimal(Int128(100i + 25), 2),
                             wide=Avro.WideDecimal(big(10)^38 + i, 3), dur=Avro.Duration(UInt32(i), UInt32(0), UInt32(7)), v=v, z=z)
        rows = [(r=inner(1, missing, 1, [(k=1,), (k=2,)]), n=missing),
                (r=inner(2, "q", 3, NamedTuple{(:k,),Tuple{Int64}}[]), n=inner(9, "w", "s", [(k=3,)]))]
        a = arrowtable(Avro.Table(Avro.tobuffer(rows; schema=s)))
        r = a.r[1]
        @test r.x === Int32(1) && ismissing(r.y) && a.r[2].y == "q" && r.e == "B" && r.f == UInt8[9, 9]
        @test r.dec === DataDecimals.Decimal{9,2,Int128}("1.25")
        @test r.wide === DataDecimals.Decimal{40,3,DataDecimals.Int256}(DataDecimals.DecimalValue{DataDecimals.Int256}(big(10)^38 + 1, 3))
        @test r.dur == Durations.Duration(1, 0, 7_000_000)
        @test [z.k for z in r.z] == [1, 2] && isempty(a.r[2].z)
        @test ismissing(a.n[1]) && a.n[2].x == 9 && a.n[2].v == "s" && a.n[2].z[1].k == 3
        @test typeof(a.n[2]) === typeof(r)
        # a union field is a union of every branch, even where the rows hold one
        @test a.r[1].v == 1 && a.r[2].v == 3 && fieldtype(typeof(r), :v) isa Union
        # back to Avro, decimals and durations keep their logical types
        back = Avro.Table(Avro.tobuffer(a))
        @test back.r[1].dec == Avro.Decimal(Int128(125), 2) && back.r[2].wide == Avro.WideDecimal(big(10)^38 + 2, 3)
        @test back.r[1].dur == Avro.Duration(UInt32(1), UInt32(0), UInt32(7))
    end

    @testset "a union value converts by the branch it came from" begin
        s = P("""{"type":"record","name":"O","fields":[{"name":"r","type":{"type":"record","name":"V","fields":[
            {"name":"d","type":[{"type":"bytes","logicalType":"decimal","precision":9,"scale":2},
                                {"type":"fixed","name":"F","size":8,"logicalType":"decimal","precision":18,"scale":3}]},
            {"name":"k","type":[{"type":"record","name":"A","fields":[{"name":"a","type":"long"}]},
                                {"type":"record","name":"B","fields":[{"name":"b","type":"string"}]}]},
            {"name":"s","type":["string",{"type":"enum","name":"E","symbols":["A","B"]}]},
            {"name":"y","type":["bytes",{"type":"fixed","name":"G","size":2}]}]}}]}""")
        enum = s.fields[1].schema.fields[3].schema.branches[2]
        rows = [(r=(d=Avro.Decimal(Int128(125), 2), k=(a=1,), s="x", y=UInt8[1, 2, 3]),),
                (r=(d=Avro.Decimal(Int128(1234), 3), k=(b="z",), s=Avro.EnumValue(enum, "B"), y=Avro.Fixed(s.fields[1].schema.fields[4].schema.branches[2], UInt8[9, 9])),)]
        a = arrowtable(Avro.Table(Avro.tobuffer(rows; schema=s)))
        @test a.r[1].d === DataDecimals.Decimal{9,2,Int128}("1.25") && a.r[2].d === DataDecimals.Decimal{18,3,Int128}("1.234")
        @test a.r[1].k == (a=1,) && a.r[2].k == (b="z",)
        # branches that Arrow stores alike share one Arrow type
        @test [r.s for r in a.r] == ["x", "B"] && !(fieldtype(eltype(a.r), :s) isa Union)
        @test [r.y for r in a.r] == [UInt8[1, 2, 3], UInt8[9, 9]] && !(fieldtype(eltype(a.r), :y) isa Union)
    end

    @testset "a missing parent leaves each field kind typed" begin
        id = uuid4()
        decimal = DataDecimals.Decimal{9,2,Int128}("1.25")
        for (field, value, stored) in (("""{"type":"string","logicalType":"uuid"}""", id, id),
                                       ("""{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}""", id, id),
                                       ("""{"type":"int","logicalType":"date"}""", Date(2024), Date(2024)),
                                       ("""{"type":"long","logicalType":"time-micros"}""", Time(1), Time(1)),
                                       ("""{"type":"long","logicalType":"timestamp-micros"}""", DateTime(2024), Avro.timestamp(Microsecond, DateTime(2024))),
                                       ("""{"type":"long","logicalType":"local-timestamp-micros"}""", DateTime(2024), Avro.localtimestamp(Microsecond, DateTime(2024))),
                                       ("""{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}""", Avro.Decimal(Int128(125), 2), decimal),
                                       ("""{"type":"fixed","name":"D","size":12,"logicalType":"duration"}""", Avro.Duration(UInt32(1), UInt32(2), UInt32(3)), Durations.Duration(1, 2, 3_000_000)),
                                       ("""{"type":"enum","name":"E","symbols":["A"]}""", "A", "A"),
                                       ("""{"type":"map","values":{"type":"map","values":"long"}}""", Dict("a" => Dict("b" => 1)), Dict("a" => Dict("b" => 1))),
                                       ("""["long","string"]""", "x", "x"),
                                       ("""[{"type":"record","name":"K","fields":[{"name":"k","type":"long"}]},"string"]""", (k=1,), (k=1,)))
            nullable = P("""{"type":"record","name":"W","fields":[{"name":"p","type":["null",{"type":"record","name":"V","fields":[{"name":"f","type":$field}]}]}]}""")
            nested = arrowtable(Avro.Table(Avro.tobuffer([(p=missing,), (p=(f=value,),)]; schema=nullable)))
            @test ismissing(nested.p[1]) && structfield(nested.p[2], :f) == stored
        end
        allmissing = Avro.Table(Avro.tobuffer([(id=1, n=missing)]; schema=P("""{"type":"record","name":"W","fields":[{"name":"id","type":"long"},
            {"name":"n","type":["null",{"type":"record","name":"V","fields":[{"name":"v","type":"long"}]}]}]}""")))
        a = arrowtable(allmissing)
        @test a.id == [1] && isequal(collect(a.n), [missing])
    end

    @testset "a value without an Arrow type is an error" begin
        function writeerror(field, rows)
            t = Avro.Table(Avro.tobuffer(rows; schema=P("""{"type":"record","name":"R","fields":[{"name":"c","type":$field}]}""")))
            try
                Arrow.write(IOBuffer(), t)
                return nothing
            catch err
                return err
            end
        end
        decimal(p) = """{"type":"bytes","logicalType":"decimal","precision":$p,"scale":0}"""
        @test writeerror("""{"type":"record","name":"V","fields":[{"name":"d","type":$(decimal(80))}]}""", [(c=(d=Avro.WideDecimal(big(1), 0),),)]) isa ArgumentError
        @test writeerror(decimal(40), [(c=Avro.WideDecimal(big(1), 0),)]) isa ArgumentError
        @test writeerror("""{"type":"fixed","name":"D","size":12,"logicalType":"duration"}""", [(c=Avro.Duration(typemax(UInt32), UInt32(0), UInt32(0)),)]) isa ArgumentError
        recursive = writeerror("""{"type":"record","name":"L","fields":[{"name":"n","type":["null","L"]}]}""", [(c=(n=(n=missing,),),)])
        @test recursive isa ArgumentError && occursin("recursive record L", sprint(showerror, recursive))
        @test writeerror("""["long","string"]""", @NamedTuple{c::Int64}[]) isa ArgumentError
    end

    @testset "partitions" begin
        s = P("""{"type":"record","name":"O","fields":[{"name":"u","type":["long","string"]},
            {"name":"n","type":["null",{"type":"record","name":"I","fields":[{"name":"y","type":["null","string"]}]}]}]}""")
        rows = [(u=1, n=missing), (u="x", n=(y="q",))]
        blocks = take!(Avro.tobuffer(Tables.partitioner([rows[1:1], rows[2:2]]); schema=s))
        @test length(Avro.Reader(r -> collect(Avro.eachblock(r)), blocks)) == 2
        # a column whose types vary by value is one batch, so the blocks need not agree
        for source in (Avro.Table(blocks), Avro.Rows(blocks))
            bytes = arrowbytes(source)
            batched = Arrow.Table(bytes)
            @test length(collect(Arrow.Stream(bytes))) == 1
            @test batched.u == [1, "x"] && ismissing(batched.n[1]) && batched.n[2].y == "q"
        end
        # rows whose schema fixes every column's type stream one batch per block, and Arrow.jl declares
        # each field of a partitioned source nullable
        fixed = P("""{"type":"record","name":"F","fields":[{"name":"a","type":"long"},{"name":"b","type":["null","string"]}]}""")
        flat = take!(Avro.tobuffer(Tables.partitioner([[(a=1, b=missing)], [(a=2, b="x")]]); schema=fixed))
        bytes = arrowbytes(Avro.Rows(flat))
        streamed = Arrow.Table(bytes)
        @test length(collect(Arrow.Stream(bytes))) == 2
        @test streamed.a == [1, 2] && isequal(streamed.b, [missing, "x"]) && eltype(streamed.a) === Union{Missing,Int64}
        @test eltype(arrowtable(Avro.Table(flat)).a) === Int64
        # an empty source still writes its columns
        empty = take!(Avro.tobuffer(NamedTuple{(:a,),Tuple{Int}}[]))
        @test collect(Tables.columnnames(arrowtable(Avro.Rows(empty)))) == [:a]
        @test length(arrowtable(Avro.Table(Avro.tobuffer(@NamedTuple{a::Int64, b::Union{Missing,String}}[]; schema=fixed))).b) == 0
    end

    @testset "Arrow tables write to Avro" begin
        id = uuid4()
        a = arrowtable((a=[1, 2], b=["x", missing], c=[[1], [2, 3]], d=[(p=1, q="z"), (p=2, q="w")], m=[Dict("k" => 1), Dict("j" => 2)],
                        id=[id, id], dec=[DataDecimals.Decimal{9,2}("1.25"), DataDecimals.Decimal{9,2}("-2.50")],
                        dur=[Durations.Duration(1, 2, 3_000_000), Durations.Duration(0, 0, 0)]))
        back = Avro.Table(Avro.tobuffer(a))
        @test back.a == [1, 2] && isequal(back.b, ["x", missing]) && back.c == [[1], [2, 3]] && back.d[2].q == "w"
        @test back.m == [Dict("k" => 1), Dict("j" => 2)] && back.id == [id, id]
        @test back.dec == [Avro.Decimal(Int128(125), 2), Avro.Decimal(Int128(-250), 2)]
        @test back.dur == [Avro.Duration(UInt32(1), UInt32(2), UInt32(3)), Avro.Duration(UInt32(0), UInt32(0), UInt32(0))]
        # an Arrow stream is a source of partitions, not a table
        stream = arrowbytes(Tables.partitioner([(a=[1, 2],), (a=[3],)]))
        @test Avro.Table(Avro.tobuffer(Arrow.Stream(stream))).a == [1, 2, 3]
    end

    @testset "CSV → Avro → Arrow → DataFrame" begin
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
