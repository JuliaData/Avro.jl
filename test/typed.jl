module TypedTestTypes
using StructUtils, Dates

struct Plain
    a::Int
    b::String
end
mutable struct Mut
    a::Int32
    b::Union{Missing,Int64}
end
struct Empty end
mutable struct MutEmpty end
struct Bits
    a::Int32
    f::Float32
    flag::Bool
end
struct Guarded
    a::Int
    Guarded(a) = (a > 0 || throw(ArgumentError("a must be positive")); new(a))
end
@enum Colour red green blue
StructUtils.@tags struct Tagged
    first::Int &(avro=(name="a",),)
    second::String &(name="b",)
end
StructUtils.@kwarg struct WithDefault
    a::Int
    extra::String = "dflt"
    opt::Union{Missing,Int} = missing
end
struct NoDefault
    a::Int
    extra::String
end
struct Optional
    a::Int
    maybe::Union{Nothing,String}
end
struct LL
    value::Int64
    next::Union{Missing,LL}
end
struct Node
    name::String
    children::Vector{Node}
end
abstract type Shape end
struct Circle <: Shape
    r::Float64
end
StructUtils.@choosetype Shape x -> Circle
StructUtils.@tags struct Lifted
    when::Date &(dateformat="yyyy-mm-dd",)
end
struct Wrapper
    id::String
end
StructUtils.structlike(::Type{Wrapper}) = false
StructUtils.lift(::Type{Wrapper}, x::AbstractString) = Wrapper("w:" * x)
struct HasWrapper
    w::Wrapper
end
struct SymField
    s::Symbol
    e::Symbol
end
struct Nested
    inner::Plain
    items::Vector{Plain}
    byname::Dict{String,Plain}
end
Base.:(==)(a::Node, b::Node) = a.name == b.name && a.children == b.children
Base.:(==)(a::Nested, b::Nested) = a.inner == b.inner && a.items == b.items && a.byname == b.byname
end

struct ResolvedDTO
    a::Float64
    b::Union{Missing,Int64}
    z::Int32
    ResolvedDTO() = error("the positional constructor must not run on the fast route")
end

struct Hooked
    a::Float64
    b::Union{Missing,Int64}
    z::Int32
end
StructUtils.lift(::Avro.AvroStyle, ::Type{Hooked}, x) = Hooked(1.0, missing, Int32(0))

@testset "Typed decoding" begin
    using .TypedTestTypes: Plain, Mut, Empty, MutEmpty, Bits, Guarded, Colour, red, green, blue, Tagged, WithDefault, NoDefault, Optional, LL, Node, Shape, Circle, Lifted, Wrapper, HasWrapper, SymField, Nested
    P = Avro.parseschema
    rs = P("""{"type":"record","name":"R","fields":[
        {"name":"a","type":"int"},{"name":"b","type":"string"},{"name":"c","type":["null","long"]},
        {"name":"d","type":{"type":"array","items":"double"}},{"name":"e","type":{"type":"map","values":"int"}},
        {"name":"f","type":{"type":"enum","name":"E","symbols":["red","green","blue"]}},{"name":"g","type":{"type":"fixed","name":"F","size":2}},
        {"name":"t","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"ch","type":"string"},{"name":"fl","type":"float"}]}""")
    bytes = Avro.encode(rs, (a=1, b="s", c=2, d=[1.5], e=Dict("k" => 3), f="green", g=UInt8[1, 2], t=DateTime(2020), ch="é", fl=0.5f0))
    plan(T, s=rs) = Avro.DatumReader(s, T).plan

    @testset "NamedTuple targets and leaf conversions" begin
        NT = NamedTuple{(:a, :b, :c, :d, :e, :f, :g, :t, :ch, :fl),Tuple{Int,String,Union{Missing,Int},Vector{Float64},Dict{String,Int},Symbol,NTuple{2,UInt8},DateTime,Char,Float64}}
        @test plan(NT) isa Avro.RecordTarget
        v = Avro.decode(rs, bytes, NT)
        @test v === NT((1, "s", 2, [1.5], Dict("k" => 3), :green, (0x01, 0x02), DateTime(2020), 'é', 0.5)) || v == NT((1, "s", 2, [1.5], Dict("k" => 3), :green, (0x01, 0x02), DateTime(2020), 'é', 0.5))
        @test v.a isa Int && v.c isa Int && v.e isa Dict{String,Int} && v.g === (0x01, 0x02) && v.fl === 0.5
        NT2 = NamedTuple{(:a, :f, :g, :e, :c),Tuple{Int8,String,Vector{UInt8},Dict{Symbol,Int32},Union{Nothing,Int64}}}
        w = Avro.decode(rs, bytes, NT2)
        @test w == (a=Int8(1), f="green", g=UInt8[1, 2], e=Dict(:k => Int32(3)), c=2)
        # enum targets
        NT3 = NamedTuple{(:f,),Tuple{Colour}}
        @test Avro.decode(rs, bytes, NT3).f === green
        other = P(replace(Avro.json(rs), "\"blue\"" => "\"violet\""))
        bv = Avro.encode(other, (a=1, b="s", c=missing, d=Float64[], e=Dict{String,Int}(), f="violet", g=UInt8[1, 2], t=DateTime(2020), ch="x", fl=1.0f0))
        @test_throws Avro.ConversionError Avro.decode(other, bv, NT3)
        # narrow integer range checks
        big = Avro.encode(rs, (a=300, b="s", c=missing, d=Float64[], e=Dict{String,Int}(), f="red", g=UInt8[1, 2], t=DateTime(2020), ch="x", fl=1.0f0))
        @test_throws Avro.ConversionError Avro.decode(rs, big, NamedTuple{(:a,),Tuple{Int8}})
        @test_throws Avro.ConversionError Avro.decode(rs, big, NamedTuple{(:a,),Tuple{UInt8}})
        @test Avro.decode(rs, big, NamedTuple{(:a,),Tuple{UInt16}}).a === UInt16(300)
        @test Avro.decode(rs, big, NamedTuple{(:a,),Tuple{UInt128}}).a === UInt128(300)
        neg = Avro.encode(P("\"long\""), -1)
        @test_throws Avro.ConversionError Avro.decode(P("\"long\""), neg, UInt64)
        @test Avro.decode(P("\"long\""), neg, Int128) === Int128(-1)
        @test Avro.decode(P("\"int\""), Avro.encode(P("\"int\""), 5), Int) === 5
        @test Avro.decode(P("\"float\""), Avro.encode(P("\"float\""), 0.5f0), Float16) === Float16(0.5)
        @test_throws Avro.ConversionError Avro.decode(P("\"string\""), Avro.encode(P("\"string\""), "ab"), Char)
        @test Avro.decode(P("\"string\""), Avro.encode(P("\"string\""), "😀"), Char) === '😀'
        @test Avro.decode(P("\"null\""), UInt8[], Nothing) === nothing
        @test Avro.decode(rs, bytes, Any) == Avro.decode(rs, bytes)
        @test Avro.decode(rs, bytes, Avro.Record) == Avro.decode(rs, bytes) && plan(Avro.Record) isa Avro.GenericTarget
        @test plan(Any) isa Avro.GenericTarget
        # nullable timestamps to DateTime, a non-optional target rejects null
        ts = P("[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]")
        @test Avro.decode(ts, Avro.encode(ts, DateTime(2021)), Union{Missing,DateTime}) == DateTime(2021)
        @test Avro.decode(ts, Avro.encode(ts, missing), Union{Nothing,DateTime}) === nothing
        @test_throws Avro.ConversionError Avro.decode(ts, Avro.encode(ts, missing), DateTime)
        @test Avro.decode(ts, Avro.encode(ts, DateTime(2021)), DateTime) == DateTime(2021)
        # timestamps out of the DateTime range
        far = Avro.encode(P("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}"), Avro.Timestamp{Millisecond}(typemax(Int64)))
        @test_throws Avro.ConversionError Avro.decode(P("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}"), far, DateTime)
    end

    @testset "struct targets: fast route never calls a constructor" begin
        ps = P("{\"type\":\"record\",\"name\":\"Plain\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"string\"}]}")
        pb = Avro.encode(ps, Plain(1, "x"))
        @test Avro.decode(ps, pb, Plain) == Plain(1, "x") && plan(Plain, ps) isa Avro.RecordTarget
        gs = P("{\"type\":\"record\",\"name\":\"Guarded\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        @test_throws ArgumentError Guarded(-5)
        g = Avro.decode(gs, Avro.encode(gs, (a=-5,)), Guarded)
        @test g.a == -5                                    # constructed with Expr(:new): the throwing constructor never ran
        ms = P("{\"type\":\"record\",\"name\":\"Mut\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":[\"null\",\"long\"]}]}")
        m = Avro.decode(ms, Avro.encode(ms, (a=1, b=missing)), Mut)
        @test m isa Mut && m.a === Int32(1) && m.b === missing
        m2 = Avro.decode(ms, Avro.encode(ms, (a=1, b=7)), Mut)
        @test m2.b === Int64(7)
        es = P("{\"type\":\"record\",\"name\":\"Empty\",\"fields\":[]}")
        @test Avro.decode(es, UInt8[], Empty) === Empty() && Avro.decode(es, UInt8[], MutEmpty) isa MutEmpty
        bs = P("{\"type\":\"record\",\"name\":\"Bits\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"f\",\"type\":\"float\"},{\"name\":\"flag\",\"type\":\"boolean\"}]}")
        @test Avro.decode(bs, Avro.encode(bs, Bits(3, 1.5f0, true)), Bits) === Bits(3, 1.5f0, true)
        # arrays and dicts of structs, nested records
        ns = P(replace("""{"type":"record","name":"Nested","fields":[{"name":"inner","type":"Plain"},{"name":"items","type":{"type":"array","items":"Plain"}},{"name":"byname","type":{"type":"map","values":"Plain"}}]}""", "\"Plain\"" => Avro.json(ps); count=1))
        nv = Nested(Plain(1, "i"), [Plain(2, "x"), Plain(3, "y")], Dict("k" => Plain(4, "z")))
        @test Avro.decode(ns, Avro.encode(ns, nv), Nested) == nv
        @test plan(Nested, ns) isa Avro.RecordTarget
        # field name tags, defaults, optional fields, missing defaults
        ts = P("{\"type\":\"record\",\"name\":\"T\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"string\"}]}")
        @test Avro.decode(ts, Avro.encode(ts, (a=1, b="x")), Tagged) == Tagged(1, "x")
        @test plan(Tagged, ts) isa Avro.RecordTarget
        as = P("{\"type\":\"record\",\"name\":\"A\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        wd = Avro.decode(as, Avro.encode(as, (a=9,)), WithDefault)
        @test wd.a == 9 && wd.extra == "dflt" && wd.opt === missing
        @test_throws ArgumentError Avro.DatumReader(as, NoDefault)
        @test Avro.decode(as, Avro.encode(as, (a=9,)), Optional) == Optional(9, nothing)
        # schema fields absent from the target are skipped under the validation mode
        xs = P("{\"type\":\"record\",\"name\":\"X\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"junk\",\"type\":{\"type\":\"array\",\"items\":\"boolean\"}},{\"name\":\"b\",\"type\":\"string\"}]}")
        xb = Avro.encode(xs, (a=1, junk=[true], b="x"))
        @test Avro.decode(xs, xb, Plain) == Plain(1, "x")
        bad = vcat(Avro.encode(P("\"long\""), 1), hex2bytes("01" * "02" * "02" * "00"), Avro.encode(P("\"string\""), "x"))  # sized block with a bad boolean
        @test_throws Avro.DataError Avro.decode(xs, bad, Plain)
        @test Avro.decode(xs, bad, Plain; validate=:fast) == Plain(1, "x")
    end

    @testset "recursion and Julia unions" begin
        ll = P("{\"type\":\"record\",\"name\":\"LL\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"},{\"name\":\"next\",\"type\":[\"null\",\"LL\"]}]}")
        v = LL(1, LL(2, LL(3, missing)))
        @test Avro.decode(ll, Avro.encode(ll, v), LL) == v
        @test plan(LL, ll) isa Avro.RecordTarget
        tree = P("{\"type\":\"record\",\"name\":\"Node\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"children\",\"type\":{\"type\":\"array\",\"items\":\"Node\"}}]}")
        n = Node("r", [Node("a", Node[]), Node("b", [Node("c", Node[])])])
        @test Avro.decode(tree, Avro.encode(tree, n), Node) == n
        u = P("[\"int\",\"string\",\"null\"]")
        @test Avro.decode(u, Avro.encode(u, Int32(3)), Union{Int32,String,Nothing}) === Int32(3)
        @test Avro.decode(u, Avro.encode(u, "s"), Union{Int32,String,Nothing}) == "s"
        @test Avro.decode(u, Avro.encode(u, missing), Union{Int32,String,Nothing}) === nothing
        @test Avro.decode(u, Avro.encode(u, Int32(3)), Union{Int64,String,Missing}) === Int64(3)
        @test plan(Union{Int32,String,Nothing}, u) isa Avro.UnionTarget
        @test plan(Union{Int32,Nothing}, u) isa Avro.SemanticTarget          # no member for the string branch
        @test_throws Avro.ConversionError Avro.decode(u, Avro.encode(u, "s"), Union{Int32,Nothing})
        @test Avro.decode(u, Avro.encode(u, Int32(3)), Union{Int32,Nothing}) === Int32(3)   # semantic route succeeds when possible
        arr = P("{\"type\":\"array\",\"items\":[\"null\",\"int\"]}")
        @test Avro.decode(arr, Avro.encode(arr, [1, missing]), Vector{Union{Nothing,Int}}) == [1, nothing]
        @test isequal(Avro.decode(arr, Avro.encode(arr, [1, missing]), Vector{Any}), [Int32(1), missing])
        @test isequal(Avro.decode(arr, Avro.encode(arr, [1, missing]), Vector{Union{Missing,Int}}), [1, missing])
    end

    @testset "semantic route and custom hooks" begin
        hs = P("{\"type\":\"record\",\"name\":\"Circle\",\"fields\":[{\"name\":\"r\",\"type\":\"double\"}]}")
        @test plan(Shape, hs) isa Avro.SemanticTarget && plan(Circle, hs) isa Avro.RecordTarget
        @test Avro.decode(hs, Avro.encode(hs, (r=4.0,)), Shape) === Circle(4.0)
        ls = P("{\"type\":\"record\",\"name\":\"Lifted\",\"fields\":[{\"name\":\"when\",\"type\":\"string\"}]}")
        @test plan(Lifted, ls) isa Avro.SemanticTarget                       # a dateformat tag affects construction
        @test Avro.decode(ls, Avro.encode(ls, (when="2020-02-03",)), Lifted) == Lifted(Date(2020, 2, 3))
        ws = P("{\"type\":\"record\",\"name\":\"HasWrapper\",\"fields\":[{\"name\":\"w\",\"type\":\"string\"}]}")
        @test plan(HasWrapper, ws) isa Avro.SemanticTarget                   # custom lift on the field type
        @test Avro.decode(ws, Avro.encode(ws, (w="x",)), HasWrapper) == HasWrapper(Wrapper("w:x"))
        # arraylike / dictlike targets outside the fast route
        arr = P("{\"type\":\"array\",\"items\":\"int\"}")
        @test Avro.decode(arr, Avro.encode(arr, [1, 2, 2]), Set{Int}) == Set([1, 2])
        @test Avro.decode(arr, Avro.encode(arr, [1, 2]), NTuple{2,Int}) === (1, 2)
        mp = P("{\"type\":\"map\",\"values\":\"int\"}")
        @test Avro.decode(mp, Avro.encode(mp, Dict("a" => 1)), Dict{String,Any}) == Dict("a" => 1)
        @test Avro.decode(mp, Avro.encode(mp, Dict("a" => 1)), Avro.Map{Int64}) == Avro.Map{Int64}([("a", 1)])
        @test plan(Avro.Map{Int64}, mp) isa Avro.MapTarget
        # generic values as StructUtils sources
        rec = Avro.decode(rs, bytes)
        @test StructUtils.make(Plain, Avro.decode(P("{\"type\":\"record\",\"name\":\"Plain\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"string\"}]}"), Avro.encode(Plain(1, "x"))), Avro.AvroStyle()) == Plain(1, "x")
        @test StructUtils.make(NamedTuple{(:f, :g),Tuple{Colour,Vector{UInt8}}}, rec, Avro.AvroStyle()) == (f=green, g=UInt8[1, 2])
        # conversion failures are ConversionErrors
        @test_throws Avro.ConversionError Avro.decode(P("\"string\""), Avro.encode(P("\"string\""), "x"), Int)
        @test_throws Avro.ConversionError Avro.decode(rs, bytes, Int)
    end

    @testset "Symbol admission on every typed path" begin
        ss = P("{\"type\":\"record\",\"name\":\"S\",\"fields\":[{\"name\":\"s\",\"type\":\"string\"},{\"name\":\"e\",\"type\":{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"p\",\"q\"]}}]}")
        sb = Avro.encode(ss, (s="hello", e="q"))
        @test Avro.decode(ss, sb, SymField) == SymField(:hello, :q) && plan(SymField, ss) isa Avro.RecordTarget
        tiny = Avro.SymbolAdmission(max_names=1)
        @test_throws Avro.LimitError Avro.decode(ss, sb, SymField; names=tiny)
        @test Avro.decode(ss, sb, SymField; names=:trusted) == SymField(:hello, :q)
        @test Avro.decode(ss, sb, SymField; names=Avro.SymbolAdmission(max_names=2)) == SymField(:hello, :q)
        # repeated values do not count twice
        adm = Avro.SymbolAdmission(max_names=2)
        for _ in 1:3
            @test Avro.decode(ss, sb, SymField; names=adm) == SymField(:hello, :q)
        end
        # map keys and the semantic route
        mp = P("{\"type\":\"map\",\"values\":\"int\"}")
        mb = Avro.encode(mp, Dict("a" => 1, "b" => 2))
        @test Avro.decode(mp, mb, Dict{Symbol,Int}) == Dict(:a => 1, :b => 2)
        @test_throws Avro.LimitError Avro.decode(mp, mb, Dict{Symbol,Int}; names=Avro.SymbolAdmission(max_names=1))
        @test_throws Avro.LimitError Avro.decode(mp, mb, Dict{Symbol,Any}; names=Avro.SymbolAdmission(max_names=1))    # semantic (Dict{Symbol,Any})
        sym = P("\"string\"")
        @test Avro.decode(sym, Avro.encode(sym, "zz"), Symbol) === :zz
        @test_throws Avro.LimitError Avro.decode(sym, Avro.encode(sym, "zz"), Symbol; names=Avro.SymbolAdmission(max_names=0))
        semantic = Vector{Symbol}                                                # arrays of Symbol: fast route
        arr = P("{\"type\":\"array\",\"items\":\"string\"}")
        @test Avro.decode(arr, Avro.encode(arr, ["a", "b"]), semantic) == [:a, :b]
        @test_throws Avro.LimitError Avro.decode(arr, Avro.encode(arr, ["a", "b"]), Set{Symbol}; names=Avro.SymbolAdmission(max_names=1))   # semantic route honours the admission object
        @test Avro.decode(arr, Avro.encode(arr, ["a", "b"]), Set{Symbol}; names=:trusted) == Set([:a, :b])
    end

    @testset "allocation budgets (kernel) and prepared readers" begin
        bs = P("{\"type\":\"record\",\"name\":\"Bits\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"f\",\"type\":\"float\"},{\"name\":\"flag\",\"type\":\"boolean\"}]}")
        reader = Avro.DatumReader(bs, Bits)
        bb = Avro.encode(bs, Bits(3, 1.5f0, true))
        @test reader(bb) === Bits(3, 1.5f0, true)
        budget = Avro.Budget(Avro.Limits(); available=1 << 40)
        Avro.addinput!(budget, 1 << 20)
        d = Avro.Decoder(bb, budget)
        kernel(plan, d, names) = (d.pos = 1; Avro.decodetyped(plan, d, names))
        measure(plan, d, names) = (kernel(plan, d, names); @allocated(kernel(plan, d, names)))   # function barrier: concrete plan type
        @test measure(reader.plan, d, reader.names) == 0
        ls = P("\"long\"")
        lr = Avro.DatumReader(ls)
        lb = Avro.encode(ls, 7)
        d2 = Avro.Decoder(lb, budget)
        gk(plan, d) = (d.pos = 1; Avro.decode(plan, d))
        gmeasure(plan, d) = (gk(plan, d); @allocated(gk(plan, d)))
        @test gmeasure(lr.plan, d2) == 0
        tasks = [Threads.@spawn reader(bb) for _ in 1:4]
        foreach(errormonitor, tasks)
        @test all(fetch(t) === Bits(3, 1.5f0, true) for t in tasks)
        @test Avro.DatumReader(bs, Bits)(IOBuffer(bb)) === Bits(3, 1.5f0, true)
        @test Avro.DatumReader(bs, Bits)(vcat(bb, bb), length(bb) + 1) == (Bits(3, 1.5f0, true), 2 * length(bb) + 1)
    end

    @testset "measured typed shells (plan §4.4, R10)" begin
        for T in (@NamedTuple{a::Int64, s::String}, @NamedTuple{a::Int64, b::Float64},
                  @NamedTuple{s::String, v::Vector{Int64}}, @NamedTuple{})
            probeshell = Avro.measuredshell(T)
            @test probeshell == (isbitstype(T) ? 0 : max(Int(Base.summarysize(Avro.emptyprobe(T))), 8))
        end
        @test Avro.measuredshell(@NamedTuple{a::Int64}) == 0
    end

    @testset "the direct typed route over resolving plans (plan §4.8, R18)" begin
        w = P("{\"type\":\"record\",\"name\":\"E1\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"drop\",\"type\":\"string\"},{\"name\":\"b\",\"type\":[\"null\",\"long\"]}]}")
        r = P("{\"type\":\"record\",\"name\":\"E1\",\"fields\":[{\"name\":\"a\",\"type\":\"double\"},{\"name\":\"b\",\"type\":[\"null\",\"long\"]},{\"name\":\"z\",\"type\":\"int\",\"default\":9}]}")
        T = @NamedTuple{a::Float64, b::Union{Missing,Int64}, z::Int32}
        dr = Avro.DatumReader(w, T; reader_schema=r)
        @test dr.plan isa Avro.ResolvedRecordTarget                              # not the semantic fallback
        @test dr(Avro.encode(w, (a=Int32(3), drop="x", b=Int64(7)))) == (a=3.0, b=7, z=Int32(9))
        @test isequal(dr(Avro.encode(w, (a=Int32(1), drop="y", b=missing))), (a=1.0, b=missing, z=Int32(9)))
        # an immutable struct target through Expr(:new), no constructor invoked
        drs = Avro.DatumReader(w, ResolvedDTO; reader_schema=r)
        @test drs.plan isa Avro.ResolvedRecordTarget
        v = drs(Avro.encode(w, (a=Int32(2), drop="q", b=Int64(5))))
        @test v.a == 2.0 && v.b == 5 && v.z == Int32(9)
        # nested resolved records recurse onto the direct route
        wn = P("{\"type\":\"record\",\"name\":\"O\",\"fields\":[{\"name\":\"in\",\"type\":{\"type\":\"record\",\"name\":\"I\",\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}}]}")
        rn = P("{\"type\":\"record\",\"name\":\"O\",\"fields\":[{\"name\":\"in\",\"type\":{\"type\":\"record\",\"name\":\"I\",\"fields\":[{\"name\":\"x\",\"type\":\"long\"}]}}]}")
        TN = @NamedTuple{in::@NamedTuple{x::Int64}}
        drn = Avro.DatumReader(wn, TN; reader_schema=rn)
        @test drn.plan isa Avro.ResolvedRecordTarget
        @test drn(Avro.encode(wn, (in=(x=Int32(4),),))) == (in=(x=Int64(4),),)
        # recursive resolved records terminate plan construction and stay on the direct route
        wll = P("{\"type\":\"record\",\"name\":\"RLL\",\"fields\":[{\"name\":\"value\",\"type\":\"int\"},{\"name\":\"next\",\"type\":[\"null\",\"RLL\"]}]}")
        rll = P("{\"type\":\"record\",\"name\":\"RLL\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"},{\"name\":\"next\",\"type\":[\"null\",\"RLL\"]}]}")
        drll = Avro.DatumReader(wll, LL; reader_schema=rll)
        @test drll.plan isa Avro.ResolvedRecordTarget
        llbytes = Avro.encode(wll, (value=Int32(1), next=(value=Int32(2), next=missing)))
        @test drll(llbytes) == LL(1, LL(2, missing))
        # reader defaults are materialised for every datum
        wdflt = P("{\"type\":\"record\",\"name\":\"D\",\"fields\":[]}")
        rdflt = P("{\"type\":\"record\",\"name\":\"D\",\"fields\":[{\"name\":\"xs\",\"type\":{\"type\":\"array\",\"items\":\"long\"},\"default\":[1]}]}")
        TD = @NamedTuple{xs::Vector{Int64}}
        drdflt = Avro.DatumReader(wdflt, TD; reader_schema=rdflt)
        x = drdflt(UInt8[])
        y = drdflt(UInt8[])
        @test x == y == (xs=Int64[1],)
        @test x.xs !== y.xs
        tiny = Avro.Limits(max_total_values=2)
        @test_throws Avro.LimitError Avro.DatumReader(wdflt; reader_schema=rdflt, limits=tiny)(UInt8[])
        @test_throws Avro.LimitError Avro.DatumReader(wdflt, TD; reader_schema=rdflt, limits=tiny)(UInt8[])
        # a custom-hooked target still takes the semantic route
        drh = Avro.DatumReader(w, Hooked; reader_schema=r)
        @test drh.plan isa Avro.SemanticTarget || !(drh.plan isa Avro.ResolvedRecordTarget)
        # results agree with the semantic route on the same bytes
        drg = Avro.DatumReader(w; reader_schema=r)
        g = drg(Avro.encode(w, (a=Int32(3), drop="x", b=Int64(7))))
        @test g.a == 3.0 && g.b == 7 && g.z == Int32(9)
    end
end
