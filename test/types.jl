module TypesTestTypes
using Avro, StructUtils, Dates, UUIDs
struct Pt; x::Float64; y::Union{Missing,Int32}; tags::Vector{String}; when::DateTime; end
@enum Color RED GREEN BLUE
struct Box{T}; v::T; end
struct Deep; b::Box{Box{Int64}}; end
StructUtils.@kwarg struct WithDefaults
    a::Int64 = 7
    b::String &(avro=(name="bee",),) = "x"
    c::Union{Missing,Float64} = missing
    d::Vector{Int32} = Int32[1, 2]
end
StructUtils.@kwarg struct BadDefault
    f::Avro.Duration = Avro.Duration(UInt32(1), UInt32(2), UInt32(3))
end
struct var"Bad-Name"; x::Int; end
struct BadField; var"my col"::Int; end
StructUtils.@tags struct Tagged; v::Int &(name="renamed",); end
struct Same; x::Int; end
module Other
struct Same; y::String; end
end
struct Pair2; a::Same; b::Other.Same; end
struct SelfRef; next::Union{Missing,SelfRef}; value::Int64; end
struct Holder; a::Box{Int64}; b::Box{Int64}; end
end

@testset "Julia type → schema (name policy)" begin
    T = TypesTestTypes
    J(x) = Avro.json(Avro.schema(x))
    P = Avro.parseschema
    ns = "Main.TypesTestTypes"
    @test J(Missing) == "\"null\"" && J(Nothing) == "\"null\"" && J(Bool) == "\"boolean\""
    @test J(Int32) == "\"int\"" && J(Int8) == "\"int\"" && J(UInt16) == "\"int\"" && J(Int64) == "\"long\"" && J(UInt64) == "\"long\""
    @test J(Float16) == "\"float\"" && J(Float32) == "\"float\"" && J(Float64) == "\"double\""
    @test J(Vector{UInt8}) == "\"bytes\"" && J(String) == "\"string\"" && J(Symbol) == "\"string\"" && J(Char) == "\"string\"" && J(SubString{String}) == "\"string\""
    @test J(NTuple{4,UInt8}) == "{\"type\":\"fixed\",\"name\":\"fixed_4\",\"size\":4}"
    @test J(Union{Missing,Int64}) == "[\"null\",\"long\"]" && J(Union{Int64,Missing}) == "[\"null\",\"long\"]"
    @test J(Union{Int32,String,Missing}) == "[\"null\",\"int\",\"string\"]"   # Julia's member order (Missing sorts first)
    @test_throws ArgumentError Avro.schema(Union{String,Symbol})
    @test_throws ArgumentError Avro.schema(Union{Missing,Nothing})
    @test_throws ArgumentError Avro.schema(Union{Int32,Int8})
    @test J(DateTime) == "{\"type\":\"long\",\"logicalType\":\"local-timestamp-millis\"}"
    @test J(Date) == "{\"type\":\"int\",\"logicalType\":\"date\"}" && J(Time) == "{\"type\":\"long\",\"logicalType\":\"time-micros\"}"
    @test J(UUID) == "{\"type\":\"string\",\"logicalType\":\"uuid\"}"
    @test J(Avro.Timestamp{Microsecond}) == "{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}"
    @test J(Avro.LocalTimestamp{Nanosecond}) == "{\"type\":\"long\",\"logicalType\":\"local-timestamp-nanos\"}"
    @test J(Avro.Duration) == "{\"type\":\"fixed\",\"name\":\"Duration\",\"size\":12,\"logicalType\":\"duration\"}"
    @test_throws ArgumentError Avro.schema(Avro.Decimal)
    @test_throws ArgumentError Avro.schema(Avro.UnionValue)
    @test_throws ArgumentError Avro.schema(Tuple{Int,Int})
    @test_throws ArgumentError Avro.schema(Any)
    @test_throws ArgumentError Avro.schema(Dict{Int,String})
    @test J(Vector{Int64}) == "{\"type\":\"array\",\"items\":\"long\"}"
    @test J(Dict{String,Vector{UUID}}) == "{\"type\":\"map\",\"values\":{\"type\":\"array\",\"items\":{\"type\":\"string\",\"logicalType\":\"uuid\"}}}"
    @test J(Avro.Map{Float64}) == "{\"type\":\"map\",\"values\":\"double\"}"
    @test J(Dict{Symbol,Int32}) == "{\"type\":\"map\",\"values\":\"int\"}"
    # enums
    @test J(T.Color) == "{\"type\":\"enum\",\"name\":\"Color\",\"namespace\":\"$ns\",\"symbols\":[\"RED\",\"GREEN\",\"BLUE\"]}"
    @test Avro.avrosymbol(T.Color, T.GREEN) == "GREEN"
    # structs, parametric names, nesting, defaults and tags
    pt = Avro.schema(T.Pt)
    @test Avro.fullname(pt) == "$ns.Pt" && [f.name for f in pt.fields] == ["x", "y", "tags", "when"]
    @test J(T.Box{Int64}) == "{\"type\":\"record\",\"name\":\"Box_Int64\",\"namespace\":\"$ns\",\"fields\":[{\"name\":\"v\",\"type\":\"long\"}]}"
    @test Avro.avroname(T.Box{T.Box{Int64}}) == ("Box_Main_TypesTestTypes_Box_Int64", ns)
    deep = Avro.schema(T.Deep)
    @test Avro.fullname(deep.fields[1].schema) == "$ns.Box_Main_TypesTestTypes_Box_Int64" && Avro.fullname(deep.fields[1].schema.fields[1].schema) == "$ns.Box_Int64"
    @test Avro.avroname(Dict{String,Vector{Int64}}) == ("Dict_String_Vector_Int64", "Base")
    @test Avro.sanitize("a-b--c") == "a_b_c" && Avro.sanitize("9x") == "_9x" && Avro.sanitize("__") == "_" && Avro.sanitize("") == "_"
    longname = Avro.avroname(NTuple{40,T.Box{Int64}})[1]
    @test sizeof(longname) == 100 + 1 + 16 && longname[101] == '_' && all(c -> c in "0123456789abcdef", longname[102:end])
    wd = Avro.schema(T.WithDefaults)
    @test [f.name for f in wd.fields] == ["a", "bee", "c", "d"]
    @test wd.fields[1].default.json == 7 && wd.fields[2].default.json == "x" && wd.fields[3].default.branch == 1 && wd.fields[4].default.span == "[1,2]"
    @test_throws ArgumentError Avro.schema(T.BadDefault)
    @test_throws ArgumentError Avro.schema(T.var"Bad-Name")
    @test_throws ArgumentError Avro.schema(T.BadField)
    @test [f.name for f in Avro.schema(T.Tagged).fields] == ["renamed"]
    @test_throws ArgumentError Avro.schema(T.Pair2)          # distinct Julia types, same fullname → error naming avroname
    e = try Avro.schema(T.Pair2); nothing catch err; err end
    @test occursin("avroname", sprint(showerror, e))
    h = Avro.schema(T.Holder)
    @test h.fields[1].schema === h.fields[2].schema && Avro.graphinfo(h).namedtypes == 2      # one definition plus a reference
    @test occursin("\"type\":\"Box_Int64\"", Avro.json(h))
    sr = Avro.schema(T.SelfRef)
    @test sr.fields[1].schema.branches[2] === sr && sr == P(Avro.json(sr))
    # NamedTuples: Record, Record_1, …; root overrides
    @test J((a=1, b=(c="x",), d=(e=true,))) == "{\"type\":\"record\",\"name\":\"Record\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":{\"type\":\"record\",\"name\":\"Record_1\",\"fields\":[{\"name\":\"c\",\"type\":\"string\"}]}},{\"name\":\"d\",\"type\":{\"type\":\"record\",\"name\":\"Record_2\",\"fields\":[{\"name\":\"e\",\"type\":\"boolean\"}]}}]}"
    @test Avro.fullname(Avro.schema(typeof((a=1,)); name="Row", namespace="ns")) == "ns.Row"
    @test Avro.fullname(Avro.schema(T.Pt; name="P2", namespace="x.y")) == "x.y.P2"
    @test_throws ArgumentError Avro.schema(T.Pt; name="bad name")
    @test_throws ArgumentError Avro.schema(T.Pt; namespace="bad namespace")
    @test_throws ArgumentError Avro.schema(typeof((var"my col"=1,)))
    # Tables.Schema
    ts = Tables.Schema((:a, :b), (Int64, Union{Missing,String}))
    @test Avro.json(Avro.schema(ts)) == "{\"type\":\"record\",\"name\":\"Record\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":[\"null\",\"string\"]}]}"
    @test [f.name for f in Avro.schema(ts; names=Dict(:a => "alpha")).fields] == ["alpha", "b"]
    @test_throws ArgumentError Avro.schema(Tables.Schema((Symbol("my col"),), (Int64,)))
    @test Avro.fullname(Avro.schema(ts; name="T", namespace="n")) == "n.T"
    # value-level schema(x)
    rec = Avro.Record(pt, Any[1.0, missing, String[], DateTime(2020)])
    @test Avro.schema(rec) === pt
    @test_throws Avro.LimitError Avro.schema(rec; limits=Avro.Limits(max_fields=1))
    ev = Avro.EnumValue(Avro.schema(T.Color), 2)
    @test Avro.schema(ev) === ev.schema && Avro.schema([ev, ev]) isa Avro.ArraySchema && Avro.schema([ev]).items == ev.schema   # imported (copied) child
    @test_throws Avro.LimitError Avro.schema(ev; limits=Avro.Limits(max_enum_symbols=1))
    @test_throws Avro.LimitError Avro.schema([ev]; limits=Avro.Limits(max_enum_symbols=1))
    fx = Avro.Fixed(Avro.FixedSchema("F", 2), UInt8[1, 2])
    @test Avro.schema(fx) === fx.schema && Avro.schema(Avro.Map([("a", fx)])).values == fx.schema
    @test_throws ArgumentError Avro.schema(Avro.EnumValue[])
    @test_throws ArgumentError Avro.schema(Any[ev, 1])
    @test_throws ArgumentError Avro.schema([ev, Avro.EnumValue(Avro.EnumSchema("Other", ["A"]), 1)])
    @test_throws ArgumentError Avro.schema(Avro.UnionValue(1, 2))
    @test_throws ArgumentError Avro.schema(Avro.Decimal(1, 0))
    @test Avro.json(Avro.schema(UInt8[1, 2])) == "\"bytes\"" && Avro.json(Avro.schema([1, 2])) == "{\"type\":\"array\",\"items\":\"long\"}" && Avro.json(Avro.schema("s")) == "\"string\""
    @test Avro.json(Avro.schema(Avro.Map([("k", 1.0)]))) == "{\"type\":\"map\",\"values\":\"double\"}"
    @test Avro.json(Avro.schema((a=1,))) == "{\"type\":\"record\",\"name\":\"Record\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}"
end

@testset "generic values" begin
    P = Avro.parseschema
    extreme_datetime = DateTime(Dates.UTM(typemin(Int64)))
    @test_throws Avro.ConversionError Avro.Timestamp{Millisecond}(extreme_datetime)
    @test_throws Avro.ConversionError Avro.LocalTimestamp{Millisecond}(extreme_datetime)
    m = Avro.Map([("b", 2), ("a", 1), ("b", 3), ("c", 4)])
    @test collect(keys(m)) == ["b", "a", "c"] && m["b"] == 3 && m["a"] == 1 && collect(values(m)) == [3, 1, 4]   # last wins, first position
    @test length(m) == 3 && haskey(m, "c") && !haskey(m, "z") && get(m, "z", 0) == 0 && haskey(m, :a) && m[:a] == 1
    @test_throws KeyError m["z"]
    @test Dict(m) == Dict("a" => 1, "b" => 3, "c" => 4)
    @test m == Avro.Map([("b", 3), ("a", 1), ("c", 4)]) && hash(m) == hash(Avro.Map([("b", 3), ("a", 1), ("c", 4)]))
    @test m == Avro.Map([("a", 1), ("b", 3), ("c", 4)]) && hash(m) == hash(Avro.Map([("a", 1), ("b", 3), ("c", 4)]))   # equality ignores insertion order (AbstractDict semantics)
    @test m != Avro.Map([("a", 1), ("b", 3)]) && m != Avro.Map([("a", 1), ("b", 3), ("c", 5)])
    @test Avro.Map{Int}([(:x, 1)])["x"] == 1 && Avro.Map{Int}() isa Avro.Map{Int} && isempty(Avro.Map{Int}())
    @test_throws ArgumentError Avro.Map([(1, 2)])
    @test sprint(show, Avro.Map([("k", 1)])) == "Avro.Map{Int64}(\"k\" => 1)"
    # big maps: sorted permutation is a correct index for every key, duplicates resolved
    n = 5000
    big = Avro.Map{Int}(("k$(i % 1234)" => i for i in 1:n))
    @test length(big) == 1234 && all(big["k$j"] == maximum(i for i in 1:n if i % 1234 == j) for j in 0:1233)
    @test all(big.keys[big.perm[i]] < big.keys[big.perm[i + 1]] for i in 1:length(big.perm) - 1)
    for sz in (0, 1, 11, 16, 17, 23, 43, 1001, 1024)
        ks = ["z" * string(i; pad=5) for i in sz:-1:1]
        mm = Avro.Map{Int}(k => i for (i, k) in enumerate(ks))
        @test length(mm) == sz && all(mm[k] == i for (i, k) in enumerate(ks))
        @test all(mm.keys[mm.perm[i]] < mm.keys[mm.perm[i + 1]] for i in 1:max(sz - 1, 0))
    end
    dups = Avro.Map{Int}("same" => i for i in 1:1000)
    @test length(dups) == 1 && dups["same"] == 1000
    # Record
    pt = P("""{"type":"record","name":"P","fields":[{"name":"x","type":"int"},{"name":"y","type":"string"}]}""")
    r = Avro.Record(pt, [1, "a"])
    @test r.x == 1 && r["y"] == "a" && r[:y] == "a" && r[2] == "a" && keys(r) == ["x", "y"] && length(r) == 2 && haskey(r, "x") && !haskey(r, :z) && get(r, :z, 0) == 0
    @test r.schema === pt && r.values == Any[1, "a"] && propertynames(r) == (:schema, :values)
    @test_throws KeyError r.z
    @test_throws ArgumentError Avro.Record(pt, [1])
    @test r == Avro.Record(P(Avro.json(pt)), [1, "a"]) && hash(r) == hash(Avro.Record(pt, [1, "a"])) && r != Avro.Record(pt, [2, "a"])
    @test sprint(show, r) == "Avro.Record(P: x=1, y=\"a\")"
    # EnumValue / Fixed / UnionValue / ordinal
    e = P("""{"type":"enum","name":"E","symbols":["A","B"]}""")
    ev = Avro.EnumValue(e, 2)
    @test String(ev) == "B" && Symbol(ev) === :B && Avro.ordinal(ev) == 1 && ev == Avro.EnumValue(e, "B") && ev != Avro.EnumValue(e, 1)
    @test ev == Avro.EnumValue(P(Avro.json(e)), 2) && hash(ev) == hash(Avro.EnumValue(e, "B"))
    @test_throws ArgumentError Avro.EnumValue(e, 3)
    @test_throws ArgumentError Avro.EnumValue(e, "C")
    @test sprint(show, ev) == "Avro.EnumValue(E.B)"
    f = Avro.FixedSchema("F", 2)
    fx = Avro.Fixed(f, UInt8[1, 2])
    @test fx == Avro.Fixed(f, [0x1, 0x2]) && fx != Avro.Fixed(f, [0x2, 0x2]) && sprint(show, fx) == "Avro.Fixed(F: 0x0102)"
    @test_throws ArgumentError Avro.Fixed(f, UInt8[1])
    u = Avro.UnionValue(2, "x")
    @test u.index == 2 && u.value == "x" && Avro.ordinal(u) == 1 && u == Avro.UnionValue(2, "x") && u != Avro.UnionValue(1, "x")
    @test_throws ArgumentError Avro.UnionValue(0, 1)
    @test sprint(show, u) == "Avro.UnionValue(2, \"x\")"
    # juliatype
    @test Avro.juliatype(P("\"null\"")) === Missing && Avro.juliatype(P("\"int\"")) === Int32 && Avro.juliatype(P("\"bytes\"")) === Vector{UInt8}
    @test Avro.juliatype(P("{\"type\":\"int\",\"logicalType\":\"date\"}")) === Date
    @test Avro.juliatype(P("{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}")) === Avro.Timestamp{Microsecond}
    @test Avro.juliatype(P("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":40}")) === Avro.WideDecimal
    @test Avro.juliatype(P("{\"type\":\"fixed\",\"name\":\"D\",\"size\":12,\"logicalType\":\"duration\"}")) === Avro.Duration
    @test Avro.juliatype(P("{\"type\":\"fixed\",\"name\":\"U\",\"size\":16,\"logicalType\":\"uuid\"}")) === UUID
    @test Avro.juliatype(P("[\"null\",\"string\"]")) === Union{Missing,String} && Avro.juliatype(P("[\"string\",\"null\"]")) === Union{Missing,String}
    @test Avro.juliatype(P("[\"int\",\"string\"]")) === Avro.UnionValue && Avro.juliatype(P("[\"null\"]")) === Avro.UnionValue && Avro.juliatype(P("[]")) === Avro.UnionValue
    @test Avro.juliatype(P("{\"type\":\"array\",\"items\":[\"null\",\"int\"]}")) === Vector{Union{Missing,Int32}}
    @test Avro.juliatype(P("{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":\"int\"}}")) === Vector{Any}
    @test Avro.juliatype(P("{\"type\":\"map\",\"values\":{\"type\":\"map\",\"values\":\"int\"}}")) === Avro.Map{Any}
    @test Avro.juliatype(pt) === Avro.Record && Avro.juliatype(e) === Avro.EnumValue && Avro.juliatype(f) === Avro.Fixed
end
