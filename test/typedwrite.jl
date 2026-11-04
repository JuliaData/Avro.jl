@enum TypedWriteColor typedwrite_red typedwrite_green

struct TypedWritePoint
    x::Int64
    label::String
end

StructUtils.@tags struct TypedWriteTagged
    value::Int64 &(name="renamed",)
end

struct TypedWriteList
    value::Int64
    next::Union{Missing,TypedWriteList}
end

struct TypedWriteSized <: AbstractVector{Int64}
    data::Vector{Int64}
end
Base.size(v::TypedWriteSized) = size(v.data)
Base.getindex(v::TypedWriteSized, i::Int) = v.data[i]
Base.IteratorSize(::Type{TypedWriteSized}) = Base.SizeUnknown()

# Containers whose iteration does not honor their declared element type.
struct TypedWriteLyingVector <: AbstractVector{Int64}
    data::Vector{Any}
end
Base.size(v::TypedWriteLyingVector) = size(v.data)
Base.getindex(v::TypedWriteLyingVector, i::Int) = v.data[i]

struct TypedWriteLyingDict <: AbstractDict{String,Int64}
    data::Dict{String,Any}
end
Base.length(d::TypedWriteLyingDict) = length(d.data)
Base.iterate(d::TypedWriteLyingDict, state...) = iterate(d.data, state...)

@testset "Typed writes encode as the generic route does" begin
    P = Avro.parseschema
    function generic(s, x)
        e = Avro.Encoder()
        try
            Avro.encodedatum!(Avro.writeplan(s), e, x, s)
        catch err
            return err
        end
        return (e.buf[1:e.pos], e.values)
    end
    function typed(s, x)
        e = Avro.Encoder()
        try
            Avro.encode!(e, s, x)
        catch err
            return err
        end
        return (e.buf[1:e.pos], e.values)
    end
    function same(a, b)
        a isa Exception || return a == b
        return typeof(a) === typeof(b) && (!(a isa Avro.EncodeError) || (a.msg == b.msg && a.path == b.path && a.schema === b.schema))
    end
    rec = """{"type":"record","name":"TW","fields":[{"name":"x","type":"long"},{"name":"label","type":"string"}]}"""
    cases = Any[
        ("\"long\"", Any[Int64(5), Int32(-3), UInt8(7), true, 1.5, big(10)^30, "x", missing, typedwrite_red]),
        ("\"int\"", Any[Int64(5), Int64(1) << 40, Int16(-2), 2.0]),
        ("\"float\"", Any[1.5f0, Float16(2), 1.5, Int64(3), (Int64(1) << 60) + 1]),
        ("\"double\"", Any[1.5, 1.5f0, Int64(3), big"1.5", (Int64(1) << 60) + 1, "x"]),
        ("\"string\"", Any["x", :sym, 'c', SubString("abc", 2), String(UInt8[0xff]), 1, Avro.EnumValue(Avro.EnumSchema("E", ["A"]), 1)]),
        ("\"bytes\"", Any[UInt8[1, 2], view(UInt8[1, 2, 3], 2:3), "x"]),
        ("""{"type":"fixed","name":"F2","size":2}""", Any[UInt8[1, 2], (0x01, 0x02), UInt8[1], (0x01,)]),
        ("""{"type":"enum","name":"TypedWriteColor","symbols":["typedwrite_red","typedwrite_green"]}""",
         Any["typedwrite_red", :typedwrite_green, typedwrite_green, "blue", 1]),
        ("\"null\"", Any[missing, nothing, 1]),
        ("""{"type":"int","logicalType":"date"}""", Any[Date(2020, 1, 2), DateTime(2020)]),
        ("""{"type":"long","logicalType":"time-micros"}""", Any[Time(1, 2, 3), Time(Nanosecond(1))]),
        ("""{"type":"long","logicalType":"timestamp-millis"}""", Any[DateTime(2020, 1, 2), Avro.timestamp(Millisecond, 5),
                                                                 Avro.timestamp(Microsecond, 5000), Avro.timestamp(Microsecond, 5001)]),
        ("""{"type":"long","logicalType":"local-timestamp-micros"}""", Any[DateTime(2020), Avro.localtimestamp(Microsecond, 7)]),
        ("""{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}""", Any[Avro.Decimal(125, 2), Avro.Decimal(125, 3)]),
        ("""{"type":"string","logicalType":"uuid"}""", Any[UUID(UInt128(7)), "00000000-0000-0000-0000-000000000007", "nope"]),
        ("""{"type":"fixed","name":"D","size":12,"logicalType":"duration"}""", Any[Avro.Duration(1, 2, 3)]),
        ("[\"null\",\"long\"]", Any[missing, nothing, Int64(4), "x", Avro.UnionValue(2, Int64(1))]),
        ("[\"long\",\"null\"]", Any[missing, Int64(4)]),
        ("""["null",{"type":"enum","name":"TypedWriteColor","symbols":["typedwrite_red","typedwrite_green"]}]""",
         Any[typedwrite_green, missing]),
        ("""["null",{"type":"enum","name":"Other","symbols":["x"]}]""", Any[typedwrite_green]),
        ("[\"int\",\"string\",\"null\"]", Any[Int32(1), "s", missing, Int64(1) << 40]),
        (rec, Any[(x=Int64(1), label="a"), (label="b", x=Int64(2)), TypedWritePoint(3, "c"), (x=Int64(1),),
                  (x="bad", label="a"), (x=Int64(1), label="a", extra=1.0), missing, [1, 2], Dict("x" => 1, "label" => "d")]),
        ("""["null",$rec]""", Any[(x=Int64(1), label="a"), missing, (x=Int64(1), label=2)]),
        ("""{"type":"record","name":"Tagged","fields":[{"name":"renamed","type":"long"}]}""", Any[TypedWriteTagged(9)]),
        ("""{"type":"record","name":"Empty","fields":[]}""", Any[(;), (a=1,), missing, (1, 2)]),
        ("""{"type":"array","items":"long"}""", Any[Int64[1, 2], Int32[3], Union{Missing,Int64}[1, missing], Int64[],
                                                   TypedWriteSized([4, 5]), (1, 2), Set([7]), "abc", ArrayLengthMismatch(2, 1),
                                                   TypedWriteLyingVector(Any[Int32(1), Int64(2)]), TypedWriteLyingVector(Any["x"])]),
        ("""{"type":"array","items":{"type":"array","items":"long"}}""", Any[Union{}[], [Union{}[]]]),
        ("""{"type":"array","items":["null","long"]}""", Any[Union{Missing,Int64}[1, missing], Any[missing, "bad"]]),
        ("""{"type":"map","values":"long"}""", Any[Dict("a" => Int64(1)), Dict(:a => 2), Dict(SubString("ab", 1) => 3),
                                                  Dict("a" => "bad"), Dict{Any,Int}(:a => 1, "a" => 2), (k=1,), Dict(1 => 2)]),
        ("""{"type":"map","values":["null","long"]}""", Any[Dict("a" => missing, "b" => Int64(1))]),
        ("""{"type":"map","values":"long"}""", Any[TypedWriteLyingDict(Dict{String,Any}("a" => Int32(1))),
                                                  TypedWriteLyingDict(Dict{String,Any}("a" => "bad"))]),
        ("""{"type":"map","values":{"type":"map","values":"long"}}""", Any[Dict{String,Union{}}(), Dict("a" => Dict{String,Union{}}())]),
        ("""{"type":"record","name":"TypedWriteList","fields":[{"name":"value","type":"long"},
            {"name":"next","type":["null","TypedWriteList"]}]}""",
         Any[TypedWriteList(1, TypedWriteList(2, missing)), (value=Int64(1), next=missing)]),
        ("""{"type":"record","name":"Nest","fields":[{"name":"xs","type":{"type":"array","items":$rec}},
            {"name":"m","type":{"type":"map","values":["null","double"]}},{"name":"any","type":["null","long"]}]}""",
         Any[(xs=[(x=Int64(1), label="a")], m=Dict("k" => 1.5), any=Int64(3)),
             (xs=[(x=Int64(1), label="a"), (x=Int64(2), label=:b)], m=Dict("k" => missing), any=missing),
             (xs=Any[(x=Int64(1), label="a"), (x="bad", label="b")], m=Dict{String,Float64}(), any=missing),
             (xs=[TypedWritePoint(1, "a")], m=Dict("k" => "bad"), any=missing)]),
    ]
    checked = 0
    for (src, values) in cases
        s = P(src)
        for x in values
            g = generic(s, x)
            t = typed(s, x)
            @test same(g, t)
            same(g, t) || @info "typed and generic routes differ" src x g t
            checked += 1
            g isa Exception && continue
            @test Avro.DatumWriter(s, typeof(x))(x) == g[1] && Avro.DatumWriter(s)(x) == g[1]
        end
    end
    @test checked > 100

    @testset "prepared writers: types, reuse, failures" begin
        s = P(rec)
        w = Avro.DatumWriter(s, @NamedTuple{x::Int64, label::String})
        @test w((x=Int64(1), label="a")) == Avro.encode(s, (x=1, label="a"))
        err = try
            w((x=Int64(1), label=:a))
        catch e
            e
        end
        @test err isa Avro.EncodeError && occursin("exact type", err.msg)
        bad = try
            Avro.DatumWriter(s, @NamedTuple{x::String, label::String})((x="1", label="a"))
        catch e
            e
        end
        @test bad isa Avro.EncodeError && bad.path == "\$.x" && bad.schema === s.fields[1].schema
        @test sprint(show, Avro.DatumWriter(s, Any)) == sprint(show, Avro.DatumWriter(s))
        any = Avro.DatumWriter(s)
        for v in ((x=Int64(1), label="a"), TypedWritePoint(1, "a"), Avro.Record(s, Any[Int64(1), "a"]))
            @test any(v) == Avro.encode(s, (x=1, label="a"))
        end
        external = Avro.Encoder()
        Avro.encode!(external, s, (x=Int64(1), label="a"))
        before = copy(external.buf[1:external.pos])
        @test_throws Avro.EncodeError w(external, (x=Int64(1), label=:bad))
        @test_throws Avro.EncodeError Avro.encode!(external, s, (x="bad", label="a"))
        @test external.buf[1:external.pos] == before && external.depth == 0 && isempty(external.path)
    end

    @testset "writers: a node per row type, failures name the row's path" begin
        s = P(rec)
        io = IOBuffer()
        rows = Any[(x=Int64(1), label="a"), TypedWritePoint(2, "b"), (label="c", x=Int64(3)), Avro.Record(s, Any[Int64(4), "d"])]
        Avro.Writer(io, s; sync=zeros(UInt8, 16)) do w
            foreach(r -> push!(w, r), rows)
        end
        @test Avro.Table(seekstart(io)).x == 1:4
        err = try
            Avro.Writer(IOBuffer(), s) do w
                push!(w, (x=Int64(1), label="a"))
                push!(w, (x=Int64(1), label=3))
            end
        catch e
            e
        end
        @test err isa Avro.EncodeError && err.path == "\$.label"
        wide = NamedTuple{Tuple(Symbol("c", i) for i in 1:40)}(Tuple(fill(Float64(i), 3) for i in 1:40))
        ws = Avro.schema(Tables.schema(wide))
        rs = Avro.RecordSchema("WideReversed"; fields=reverse(collect(ws.fields)))
        for schema in (ws, rs)
            cells = take!(Avro.tobuffer(wide; schema=schema, sync=zeros(UInt8, 16)))
            @test cells == take!(Avro.tobuffer(Tables.rowtable(wide); schema=schema, sync=zeros(UInt8, 16)))
        end
        badwide = merge(wide, (c7=["x", "y", "z"],))
        cellerr = try
            Avro.tobuffer(badwide; schema=ws)
        catch e
            e
        end
        @test cellerr isa Avro.EncodeError && cellerr.path == "\$.c7" && cellerr.schema === ws.fields[7].schema
    end
end
