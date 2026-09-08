# 1.x regression port (plan §9.16): the datum-level cases of the Avro.jl 1.1.2 test suite re-expressed
# through the 2.0 API (`Avro.encode(x)` under the conventional schema, `Avro.decode(schema, bytes, T)`).
# The container cases (`tobuffer`/`readtable`) follow the container reader and writer in Phase 4b.

@enum LegacyEnum hey there stranger

struct LegacyPerson
    id::Int
    name::String
end

@testset "1.x round-trip cases" begin
    function rt(x, ::Type{T}=typeof(x)) where {T}
        return Avro.decode(Avro.schema(T), Avro.encode(x), T)
    end
    @test isempty(Avro.encode(missing))
    @test Avro.encode(true) == [0x01] && Avro.encode(false) == [0x00]
    @test Avro.encode(1) == [0x02] && Avro.encode(63) == [0x7e] && Avro.encode(64) == [0x80, 0x01]
    @test Avro.encode(-1) == [0x01] && Avro.encode(-63) == [0x7d] && Avro.encode(-64) == [0x7f] && Avro.encode(-65) == [0x81, 0x01]
    @test rt(typemax(UInt8)) === typemax(UInt8)
    w16 = Avro.DatumWriter(Avro.schema(Int16))
    r16 = Avro.DatumReader(Avro.schema(Int16), Int)
    @test all(i == r16(w16(i)) for i in typemin(Int16):typemax(Int16))
    for x in (-0.0001, 0.0, -0.0, 1.0, floatmin(Float32), floatmax(Float32), floatmin(Float64), floatmax(Float64))
        @test x === rt(x)
    end
    @test rt(Vector{UInt8}("hey there stranger")) == Vector{UInt8}("hey there stranger") && rt(UInt8[]) == UInt8[]
    @test rt("hey there stranger") == "hey there stranger" && rt("") == ""
    @test rt([1, 2, 3, 4, 5]) == [1, 2, 3, 4, 5] && rt(["hey", "there", "stranger"]) == ["hey", "there", "stranger"] && rt(String[]) == String[]
    @test rt(tuple(b"hey"...)) === tuple(b"hey"...) && rt(()) === ()
    @test rt(Dict("hey" => 1, "there" => 2, "stranger" => 3)) == Dict("hey" => 1, "there" => 2, "stranger" => 3) && rt(Dict{String,Int}()) == Dict{String,Int}()
    @test rt(hey) === hey && rt(stranger) === stranger
    us = Avro.schema(Union{Int,String})
    @test Avro.decode(us, Avro.encode(us, 1), Union{Int,String}) === 1 && Avro.decode(us, Avro.encode(us, "s"), Union{Int,String}) == "s"
    nt = (a=1, b=3.4, c="hey")
    @test rt(nt) === nt
    r = Avro.decode(Avro.schema(typeof(nt)), Avro.encode(nt))
    @test r.a == 1 && r.b == 3.4 && r.c == "hey"
    p = LegacyPerson(10, "Valentin")
    @test rt(p) === p
    ps = [LegacyPerson(1, "meg"), LegacyPerson(2, "jo"), LegacyPerson(3, "beth"), LegacyPerson(4, "amy")]
    @test rt(ps) == ps
    ds = Avro.parseschema("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":4,\"scale\":0}")
    @test Avro.decode(ds, Avro.encode(ds, Avro.Decimal(1, 0))) == Avro.Decimal(1, 0)
    u = UUID(0x0123456789abcdef0123456789abcdef)
    @test rt(u) === u
    @test rt(Date(2021, 3, 4)) === Date(2021, 3, 4)
    @test rt(Time(1, 2, 3, 4, 5)) === Time(1, 2, 3, 4, 5)                 # microsecond-aligned (time-micros)
    @test rt(DateTime(2021, 3, 4, 5, 6, 7, 8)) === DateTime(2021, 3, 4, 5, 6, 7, 8)
    @test rt(Avro.Duration(1, 2, 3)) === Avro.Duration(1, 2, 3)
    # combinations (Char-keyed dictionaries of 1.x are string-keyed in 2.0; enums are Base.Enum subtypes)
    cases = Any[
        [missing, missing, missing],
        [true, false, true],
        [1.2, 3.4, 5.6],
        [Vector{UInt8}("hey"), Vector{UInt8}("there"), Vector{UInt8}("stranger")],
        [hey, there, stranger],
        [[1, 2], [3, 4, 5], [6, 7, 8, 9]],
        [Dict(:a => Float32(1)), Dict(:b => Float32(2)), Dict(:c => Float32(3))],
        [(a=Date(2021, 1, 1), b=true), (a=Date(2021, 1, 2), b=false), (a=Date(2021, 1, 3), b=true)],
        Union{Missing,UUID,Avro.Duration}[UUID(0x1234), missing, Avro.Duration(4, 5, 6)],
        # 1.x accepted `Union{Int32,Date}` (two `int` branches, a schema Avro and Java reject); 2.0 raises, so the member is a long
        Union{Missing,Int64,Vector{UInt8},Date,UUID,Dict{String,NamedTuple{(:a,),Tuple{Union{Int64,Float32}}}},Vector{Union{NamedTuple{(:a,),Tuple{Int64}},LegacyEnum}}}[
            missing, Int64(4), Vector{UInt8}("hey"), Date(2021, 2, 1), UUID(0x5678),
            Dict{String,NamedTuple{(:a,),Tuple{Union{Int64,Float32}}}}("a" => (a=Int64(1),), "b" => (a=Float32(3.14),)),
            Union{NamedTuple{(:a,),Tuple{Int64}},LegacyEnum}[(a=1001,), hey, there]],
        Dict("a" => missing),
        Dict("a" => true, "b" => false),
        Dict("a" => 1.2, "b" => 3.4),
        Dict("a" => Vector{UInt8}("hey"), "b" => Vector{UInt8}("there")),
        Dict("a" => hey, "b" => there),
        Dict("a" => [1, 2], "b" => [3, 4, 5]),
        Dict("a" => Dict(:a => Float32(1)), "b" => Dict(:b => Float32(2))),
        Dict("a" => (a=Time(1, 2, 3), b=UUID(0x9abc)), "b" => (a=Time(4, 5, 6), b=UUID(0xdef0))),
        (a=missing,), (a=[missing, missing],), (a=Dict("a" => missing),), (a=Dict("a" => [missing, missing]),),
        (a=true, b=false), (a=[true, false], b=[false, false]), (a=Dict("a" => true),), (a=Dict("a" => [true, false], "b" => [false, false]),),
        (a=1.2,), (a=[1.2, 3.4],), (a=Dict("a" => 4.5),), (a=Dict("a" => [6.7, 8.9]),),
        (a=Vector{UInt8}("hey"), b=hey, c=[1, 2]),
        (a=Dict("a" => Float32(1.2)),),
        (a=(a=Date(2021, 1, 1), b=Time(1, 2, 3), c=UUID(0x1111)),),
        NamedTuple{(:a,),Tuple{Union{Missing,UUID,Dict{String,Float32}}}}((Dict("a" => Float32(1.4)),)),
    ]
    for case in cases
        @test isequal(case, rt(case))
    end
    # a decimal inside a union needs its explicit schema (no conventional decimal schema in 2.0)
    ms = Avro.parseschema("{\"type\":\"map\",\"values\":[\"null\",{\"type\":\"long\",\"logicalType\":\"local-timestamp-millis\"},{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":5,\"scale\":1}]}")
    mv = Dict{String,Union{Missing,DateTime,Avro.Decimal}}("a" => missing, "b" => DateTime(2021, 3, 4, 5, 6, 7, 8), "c" => Avro.Decimal(12345, 1))
    @test isequal(Avro.decode(ms, Avro.encode(ms, mv), typeof(mv)), mv)
end
