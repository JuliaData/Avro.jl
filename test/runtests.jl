using Test, Avro, UUIDs, Dates, StructTypes, JSON3, Tables, SentinelArrays

@testset "Avro.jl" begin

# missing
buf = Avro.write(missing)
@test isempty(buf)

# booleans
buf = Avro.write(true)
@test buf[1] == 0x01
buf = Avro.write(false)
@test buf[1] == 0x00

# integers
buf = Avro.write(1)
@test buf[1] == 0x02
buf = Avro.write(63)
@test buf[1] == 0x7e
buf = Avro.write(64)
@test buf == [0x80, 0x01]

x = typemax(UInt8)
@test Avro.read(Avro.write(x), UInt8) === x

buf = Avro.write(-1)
@test buf[1] == 0x01
buf = Avro.write(-63)
@test buf[1] == 0x7d
buf = Avro.write(-64)
@test buf[1] == 0x7f
buf = Avro.write(-65)
@test buf == [0x81, 0x01]

for i = typemin(Int16):typemax(Int16)
    @test i == Avro.read(Avro.write(i), Int)
end

# floats
for x in (-0.0001, 0.0, -0.0, 1.0, floatmin(Float32), floatmax(Float32), floatmin(Float64), floatmax(Float64))
    @test x === Avro.read(Avro.write(x), typeof(x))
end

# bytes
x = [UInt8(y) for y in "hey there stranger"]
@test x == Avro.read(Avro.write(x), typeof(x))

x = UInt8[]
@test x == Avro.read(Avro.write(x), typeof(x))

# strings
x = "hey there stranger"
@test x == Avro.read(Avro.write(x), typeof(x))

x = ""
@test x == Avro.read(Avro.write(x), typeof(x))

# array
x = [1, 2, 3, 4, 5]
@test x == Avro.read(Avro.write(x), typeof(x))

# array of strings
x = ["hey", "there", "stranger"]
@test x == Avro.read(Avro.write(x), typeof(x))

x = String[]
@test x == Avro.read(Avro.write(x), typeof(x))

# fixed
x = tuple(b"hey"...)
@test x == Avro.read(Avro.write(x), typeof(x))

x = ()
@test x == Avro.read(Avro.write(x), typeof(x))

# maps
x = Dict("hey" => 1, "there" => 2, "stranger" => 3)
@test x == Avro.read(Avro.write(x), typeof(x))

x = Dict{String, Int}()
@test x == Avro.read(Avro.write(x), typeof(x))

# enums
x = Avro.Enum{(:hey, :there, :stranger)}(0)
@test x == Avro.read(Avro.write(x), typeof(x))

# unions
x = 1
@test x == Avro.read(Avro.write(x; schema=Union{Int, String}), Union{Int, String})

# records
x = (a=1, b=3.4, c="hey")
@test x == Avro.read(Avro.write(x), typeof(x))

r = Avro.read(Avro.write(x), Avro.Record{(:a, :b, :c), Tuple{Int, Float64, String}})
@test r.a == 1
@test r.b == 3.4
@test r.c == "hey"

struct Person
    id::Int
    name::String
end

StructTypes.StructType(::Type{Person}) = StructTypes.Struct()

x = Person(10, "Valentin")
@test x == Avro.read(Avro.write(x), typeof(x))

x = [Person(1, "meg"), Person(2, "jo"), Person(3, "beth"), Person(4, "amy")]
@test x == Avro.read(Avro.write(x), typeof(x))

# logical
x = Avro.Decimal{0, 4}(Int128(1))
@test x == Avro.read(Avro.write(x), typeof(x))

x = UUID(rand(UInt128))
@test x == Avro.read(Avro.write(x), typeof(x))

x = Dates.today()
@test x == Avro.read(Avro.write(x), typeof(x))

x = Time(Dates.now())
@test x == Avro.read(Avro.write(x), typeof(x))

x = Dates.now()
@test x == Avro.read(Avro.write(x), typeof(x))

x = Avro.Duration(1, 2, 3)
@test x == Avro.read(Avro.write(x), typeof(x))

# combinations
cases = [
    # arrays
    [missing, missing, missing],
    [true, false, true],
    [1.2, 3.4, 5.6],
    [Vector{UInt8}("hey"), Vector{UInt8}("there"), Vector{UInt8}("stranger")],
    [Avro.Enum{(:a, :b, :c)}(0), Avro.Enum{(:a, :b, :c)}(1), Avro.Enum{(:a, :b, :c)}(2)],
    [[1, 2], [3, 4, 5], [6, 7, 8, 9]],
    [Dict(:a => Float32(1)), Dict(:b => Float32(2)), Dict(:c => Float32(3))],
    [(a=Date(2021, 1, 1), b=true), (a=Date(2021, 1, 2), b=false), (a=Date(2021, 1, 3), b=true)],
    Union{Missing, UUID, Avro.Duration}[UUID(rand(UInt128)), missing, Avro.Duration(4, 5, 6)],
    Union{Missing, Int32, Vector{UInt8}, Date, UUID, Dict{String, NamedTuple{(:a,), Tuple{Union{Int64, Float32}}}}, Vector{Union{NamedTuple{(:a,), Tuple{Int64}}, Avro.Enum{(:a, :b)}}}}[missing, Int32(4), Vector{UInt8}("hey"), Date(2021, 2, 1), UUID(rand(UInt128)), Dict{String, NamedTuple{(:a,), Tuple{Union{Int64, Float32}}}}("a" => (a=Int64(1),), "b" => (a=Float32(3.14),)), Union{NamedTuple{(:a,), Tuple{Int64}}, Avro.Enum{(:a, :b)}}[(a=1001,), Avro.Enum{(:a, :b)}(0), Avro.Enum{(:a, :b)}(1)]],
    # maps
    Dict("a" => missing),
    Dict("a" => true, "b" => false),
    Dict("a" => 1.2, "b" => 3.4),
    Dict("a" => Vector{UInt8}("hey"), "b" => Vector{UInt8}("there")),
    Dict("a" => Avro.Enum{(:a, :b, :c)}(0), "b" => Avro.Enum{(:a, :b, :c)}(1)),
    Dict("a" => [1, 2], "b" => [3, 4, 5]),
    Dict("a" => Dict(:a => Float32(1)), "b" => Dict(:b => Float32(2))),
    Dict("a" => (a=Time(1, 2, 3), b=UUID(rand(UInt128))), "b" => (a=Time(4, 5, 6), b=UUID(rand(UInt128)))),
    Dict{String, Union{Missing, DateTime, Avro.Decimal{1, 4}}}("a" => missing, "b" => Dates.now(), "c" => Avro.Decimal{1, 4}(12345)),
    # records
    (a=missing,),
    (a=[missing, missing],),
    (a=Dict("a" => missing),),
    (a=Dict("a" => [missing, missing]),),
    (a=true, b=false),
    (a=[true, false], b=[false, false]),
    (a=Dict("a" => true),),
    (a=Dict("a" => [true, false], "b" => [false, false]),),
    (a=1.2,),
    (a=[1.2, 3.4],),
    (a=Dict("a" => 4.5),),
    (a=Dict("a" => [6.7, 8.9]),),
    (a=Vector{UInt8}("hey"), b=Avro.Enum{(:a, :b)}(0), c=[1, 2]),
    (a=Dict('a' => Float32(1.2)),),
    (a=(a=Date(2021, 1, 1), b=Time(1, 2, 3), c=UUID(rand(UInt128))),),
    NamedTuple{(:a,), Tuple{Union{Missing, UUID, Dict{String, Float32}}}}((Dict("a" => Float32(1.4)),)),
]

for case in cases
    @test isequal(case, Avro.read(Avro.write(case), typeof(case)))
    # write out schema, object to file from julia
    # sch = Avro.schematype(typeof(case))
    # JSON3.write("schema.avsc", sch)
    # Avro.write("x.avro", case)
    # js = """
    #     let avro = require('avro-js');
    #     let fs = require('fs');
    #     // read schema, object from file in node
    #     let sch = avro.parse('./schema.avsc');
    #     let x = sch.fromBuffer(fs.readFileSync('./x.avro'));
    #     // write out schema, object to file from node
    #     fs.writeFileSync('jsschema.avsc', sch.getSchema());
    #     fs.writeFileSync('jsx.avro', sch.toBuffer(x));
    # """
    # run(`node -e $js`)
    # # read in schema, object from file in julia
    # sch2 = Avro.parseschema("jsschema.avsc"))
    # case2 = Avro.read(Base.read("jsx.avro"), sch)
    # @test sch == sch2
    # @test isequal(case, case2)
end

nt = (a=1, b=2, c=3)
rt = [nt, nt, nt]
for comp in (:deflate, :bzip2, :xz, :zstd)
    io = Avro.tobuffer(rt; compress=comp)
    tbl = Avro.readtable(io)
    @test length(tbl) == 3
    @test tbl[1].a == nt.a
    @test tbl[1].b == nt.b
    @test tbl[1].c == nt.c
end

nt = (a=[1, 2, 3], b=[4.0, 5.0, 6.0], c=["7", "8", "9"])
io = Avro.tobuffer(nt)
tbl = Avro.readtable(io)
@test length(tbl) == 3
@test tbl.sch == Tables.Schema((:a, :b, :c), (Int, Float64, String))

rt = [(a=1, b=4.0, c="7"), (a=2.0, b=missing, c="8"), (a=3, b=6.0, c="9")]
io = Avro.tobuffer(rt)
tbl = Avro.readtable(io)
@test length(tbl) == 3

rt = [
    (a=1, b=2, c=3),
    (b=4.0, c=missing, d=5),
    (a=6, d=7),
    (a=8, b=9, c=10, d=missing),
    (d=11, c=10, b=9, a=8)
]

dct = Tables.dictcolumntable(rt)
io = Avro.tobuffer(dct)
tbl = Avro.readtable(io)
@test length(tbl) == 5

end

@testset "rowwise" begin
    rows = [
        (a=missing, b=2., c=3, d=4),
        (a=1, b=4.0, c=missing, d=5),
        (a=1, b=4.0, c=missing, d="5"),
    ]
    sch = Tables.Schema([:a, :b, :c, :d], [Union{Missing, Int64}, Float64, Union{Missing, Int64}, Union{Int64, String}])

    mktempdir() do p
        pth = joinpath(p,"tmp.avro")
        Avro.recordwriter(sch, pth) do rw
            Avro.writerecord(rw, rows[1])
            Avro.writerecord(rw, rows[2])
            Avro.writerecord(rw, rows[3])
        end
        
        Avro.recordreader(pth) do rr
            @test all(collect(skipmissing(first(rr))) .== collect(skipmissing(rows[1])))
            @test all(collect(skipmissing(first(rr))) .== collect(skipmissing(rows[1])))
            res= []
            for r in rr
                push!(res, NamedTuple(r))
            end
            @test all(collect.(skipmissing.(res)).==collect.(skipmissing.(rows)))
        end
        tbl = Avro.readtable(pth)
        @test length(tbl) == 3
        @test tbl.sch == sch

        pth = joinpath(p,"tmp.avro")
        Avro.recordwriter(sch, pth; compress=:bzip2) do rw
            Avro.writerecord(rw, rows[1])
        end
        tbl = Avro.readtable(pth)
        f = first(tbl)
        @test f.b == 2.
    end
end


# using CSV, Dates, Tables, Test
# const dir = joinpath(dirname(pathof(CSV)), "..", "test", "testfiles")
# include(joinpath(dirname(pathof(CSV)), "..", "test", "testfiles.jl"))
# for (i, test) in enumerate(testfiles)
#     file, kwargs, expected_sz, expected_sch, testfunc = test
#     println("testing $file, i = $i")
#     f = CSV.File(file isa IO ? file : joinpath(dir, file); kwargs...)
#     buf = Avro.tobuffer(f)
#     tbl = Avro.readtable(buf)
#     @test isequal(columntable(f), columntable(tbl))
# end
