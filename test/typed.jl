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
    function Guarded(a)
        a > 0 || throw(ArgumentError("a must be positive"))
        return new(a)
    end
end
@enum Colour red green blue
struct EnumFallback
    f::Colour
    bad::Int64
end

const TransactionChild = @NamedTuple{f::Colour}

struct TransactionFail
    child::TransactionChild
    bad::Int64
end

struct TransactionGood
    child::TransactionChild
    bad::String
end

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

function StructUtils.structlike(::Type{Wrapper})
    return false
end

function StructUtils.lift(::Type{Wrapper}, x::AbstractString)
    return Wrapper("w:" * x)
end
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

function Base.:(==)(a::Node, b::Node)
    return a.name == b.name && a.children == b.children
end

function Base.:(==)(a::Nested, b::Nested)
    return a.inner == b.inner && a.items == b.items && a.byname == b.byname
end
end

struct ResolvedDTO
    a::Float64
    b::Union{Missing,Int64}
    z::Int32
    function ResolvedDTO()
        return error("the positional constructor must not run on the fast route")
    end
end

struct Hooked
    a::Float64
    b::Union{Missing,Int64}
    z::Int32
end

function StructUtils.lift(::Avro.AvroStyle, ::Type{Hooked}, x)
    return Hooked(1.0, missing, Int32(0))
end

mutable struct ShellMut
    a::Int64
    s::String
end

struct ShellPadded
    a::Int8
    b::Int64
    s::String
end

mutable struct InfiniteMapNode
    next::InfiniteMapNode
end

struct OversizedTypedFieldName
    x::Int64
end

struct InterruptingSummary
    value::String
end

function Base.summarysize(::InterruptingSummary)
    throw(InterruptException())
end

struct FailingSummary
    value::String
end

const SUMMARY_SENTINEL = ArgumentError("summarysize sentinel")

function Base.summarysize(::FailingSummary)
    throw(SUMMARY_SENTINEL)
end

struct DefaultProbe end
struct DefaultProbeError <: Exception end

function Avro.convertleaf(::Type{DefaultProbe}, ::String, budget)
    throw(DefaultProbeError())
end

struct BadSemanticHook end
mutable struct NestedBadSemanticHook
    value::BadSemanticHook
    function NestedBadSemanticHook()
        return new()
    end
end
const SEMANTIC_HOOK_SENTINEL = ArgumentError("semantic hook sentinel")

function StructUtils.lift(::Avro.AvroStyle, ::Type{BadSemanticHook}, x)
    throw(SEMANTIC_HOOK_SENTINEL)
end

const OVERSIZED_TYPED_FIELD_SOURCE = "x" * repeat("n", 1024)
const OVERSIZED_TYPED_FIELD_NAME = SubString(OVERSIZED_TYPED_FIELD_SOURCE, 2)

function StructUtils.fieldtags(::Avro.AvroStyle, ::Type{OversizedTypedFieldName})
    return (x=(name=OVERSIZED_TYPED_FIELD_NAME,),)
end

@testset "Typed decoding" begin
    using .TypedTestTypes: Plain, Mut, Empty, MutEmpty, Bits, Guarded, Colour, red, green, blue, EnumFallback, TransactionFail, TransactionGood, Tagged, WithDefault, NoDefault, Optional, LL, Node, Shape, Circle, Lifted, Wrapper, HasWrapper, SymField, Nested
    P = Avro.parseschema
    rs = P("""{"type":"record","name":"R","fields":[
        {"name":"a","type":"int"},{"name":"b","type":"string"},{"name":"c","type":["null","long"]},
        {"name":"d","type":{"type":"array","items":"double"}},{"name":"e","type":{"type":"map","values":"int"}},
        {"name":"f","type":{"type":"enum","name":"E","symbols":["red","green","blue"]}},{"name":"g","type":{"type":"fixed","name":"F","size":2}},
        {"name":"t","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"ch","type":"string"},{"name":"fl","type":"float"}]}""")
    bytes = Avro.encode(rs, (a=1, b="s", c=2, d=[1.5], e=Dict("k" => 3), f="green", g=UInt8[1, 2], t=DateTime(2020), ch="é", fl=0.5f0))
    function plan(T, s=rs)
        return Avro.DatumReader(s, T).plan
    end

    @testset "NamedTuple targets and leaf conversions" begin
        NT = NamedTuple{(:a, :b, :c, :d, :e, :f, :g, :t, :ch, :fl),Tuple{Int,String,Union{Missing,Int},Vector{Float64},Dict{String,Int},Symbol,NTuple{2,UInt8},DateTime,Char,Float64}}
        @test plan(NT) isa Avro.SemanticTarget       # the Dict field converts after generic ownership transfer
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
        longchar = "x"^100_000
        charbudget = Avro.Budget(Avro.Limits(); available=1 << 30)
        charerror = try
            Avro.convertleaf(Char, longchar, charbudget)
            nothing
        catch caught
            caught
        end
        @test charerror isa Avro.ConversionError
        @test sizeof(charerror.msg) < 256
        @test occursin("+99904 bytes", charerror.msg)
        @test charbudget.reserved == Avro.stringbytes(sizeof(charerror.msg))
        Avro.close!(charbudget)
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
        @test plan(Nested, ns) isa Avro.SemanticTarget   # nested Dict also forces the whole DTO to the semantic route
        # field name tags, defaults, optional fields, missing defaults
        ts = P("{\"type\":\"record\",\"name\":\"T\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"string\"}]}")
        @test Avro.decode(ts, Avro.encode(ts, (a=1, b="x")), Tagged) == Tagged(1, "x")
        @test plan(Tagged, ts) isa Avro.RecordTarget
        as = P("{\"type\":\"record\",\"name\":\"A\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        namelimits = Avro.Limits(max_name_bytes=4)
        @test_throws Avro.LimitError Avro.DatumReader(as, OversizedTypedFieldName;
                                                       limits=namelimits)
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
        countedunion = P("[\"int\",\"string\"]")
        countedbytes = UInt8[0x00, 0x00]
        @test Avro.decode(countedunion, countedbytes, Union{Int32,String};
                          limits=Avro.Limits(max_total_values=2)) === Int32(0)
        @test_throws Avro.LimitError Avro.decode(countedunion, countedbytes, Union{Int32,String};
                                                  limits=Avro.Limits(max_total_values=1))
        countednullable = P("[\"null\",\"int\"]")
        @test Avro.decode(countednullable, UInt8[0x00], Union{Missing,Int32};
                          limits=Avro.Limits(max_total_values=2)) === missing
        @test_throws Avro.LimitError Avro.decode(countednullable, UInt8[0x00], Union{Missing,Int32};
                                                  limits=Avro.Limits(max_total_values=1))
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
        @test plan(Dict{String,Int64}, mp) isa Avro.SemanticTarget
        @test plan(Avro.Map{Int64}, mp) isa Avro.MapTarget
        # generic values as StructUtils sources
        rec = Avro.decode(rs, bytes)
        @test StructUtils.make(Plain, Avro.decode(P("{\"type\":\"record\",\"name\":\"Plain\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"string\"}]}"), Avro.encode(Plain(1, "x"))), Avro.AvroStyle()) == Plain(1, "x")
        @test StructUtils.make(NamedTuple{(:f, :g),Tuple{Colour,Vector{UInt8}}}, rec, Avro.AvroStyle()) == (f=green, g=UInt8[1, 2])
        # conversion failures are ConversionErrors
        @test_throws Avro.ConversionError Avro.decode(P("\"string\""), Avro.encode(P("\"string\""), "x"), Int)
        @test_throws Avro.ConversionError Avro.decode(rs, bytes, Int)

        hookbytes = Avro.encode(Avro.IntSchema(), Int32(1))
        root_hook_error = try
            Avro.decode(Avro.IntSchema(), hookbytes, BadSemanticHook)
            nothing
        catch err
            err
        end
        @test root_hook_error === SEMANTIC_HOOK_SENTINEL
        nested_hook_schema = P("{\"type\":\"record\",\"name\":\"NestedBadSemanticHook\",\"fields\":[{\"name\":\"value\",\"type\":\"int\"}]}")
        nested_hook_bytes = Avro.encode(nested_hook_schema, (value=Int32(1),))
        nested_hook_error = try
            Avro.decode(nested_hook_schema, nested_hook_bytes, NestedBadSemanticHook)
            nothing
        catch err
            err
        end
        @test nested_hook_error === SEMANTIC_HOOK_SENTINEL
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
        function kernel(plan, d, names)
            d.pos = 1
            return Avro.decodetyped(plan, d, names)
        end

        function measure(plan, d, names)
            kernel(plan, d, names)
            return @allocated(kernel(plan, d, names))   # function barrier: concrete plan type
        end
        @test measure(reader.plan, d, reader.names) == 0
        ls = P("\"long\"")
        lr = Avro.DatumReader(ls)
        lb = Avro.encode(ls, 7)
        d2 = Avro.Decoder(lb, budget)
        function gk(plan, d)
            d.pos = 1
            return Avro.decode(plan, d)
        end

        function gmeasure(plan, d)
            gk(plan, d)
            return @allocated(gk(plan, d))
        end
        @test gmeasure(lr.plan, d2) == 0
        tasks = [Threads.@spawn reader(bb) for _ in 1:4]
        foreach(errormonitor, tasks)
        @test all(fetch(t) === Bits(3, 1.5f0, true) for t in tasks)
        @test Avro.DatumReader(bs, Bits)(IOBuffer(bb)) === Bits(3, 1.5f0, true)
        @test Avro.DatumReader(bs, Bits)(vcat(bb, bb), length(bb) + 1) == (Bits(3, 1.5f0, true), 2 * length(bb) + 1)
    end

    @testset "measured typed shells: layouts, marginals, true-up (plan §4.4, round-2 D06)" begin
        for T in (@NamedTuple{a::Int64, s::String}, @NamedTuple{a::Int64, b::Float64},
                  @NamedTuple{s::String, v::Vector{Int64}}, @NamedTuple{}, ShellMut, ShellPadded)
            probeshell = Avro.measuredshell(T)
            @test probeshell == (isbitstype(T) ? 0 : max(Int(Base.summarysize(Avro.emptyprobe(T))), 8))
        end
        @test Avro.measuredshell(@NamedTuple{a::Int64}) == 0
        # an inline nested struct charges its own shell: the outer measures the marginal
        Outer = @NamedTuple{x::Int64, inner::@NamedTuple{s::String, y::Int64}}
        Inner = @NamedTuple{s::String, y::Int64}
        expectedshell = max(Int(Base.summarysize(Avro.emptyprobe(Outer))), 8)
        VERSION < v"1.11" && (expectedshell += Avro.measuredshell(Inner)) # Julia 1.10's oracle counts the inline child again
        @test Avro.measuredshell(Outer) + Avro.measuredshell(Inner) == expectedshell
        # the probe bound reserves and trues up to zero on a live budget
        b = Avro.Budget(Avro.Limits())
        r0 = b.reserved
        Avro.measuredshell(Outer, b)
        @test b.reserved == r0
        Avro.close!(b)
        @test_throws InterruptException Avro.measuredshell(InterruptingSummary)
        @test_throws ArgumentError Avro.measuredshell(FailingSummary)
        summarybudget = Avro.Budget(Avro.Limits(); available=1 << 40)
        @test_throws InterruptException Avro.measuredinlineshell(InterruptingSummary,
                                                                 summarybudget)
        @test summarybudget.reserved == summarybudget.pending == 0
        summaryerror = try
            Avro.measuredinlineshell(FailingSummary, summarybudget)
            nothing
        catch err
            err
        end
        @test summaryerror === SUMMARY_SENTINEL
        @test summarybudget.reserved == summarybudget.pending == 0
        Avro.close!(summarybudget)
        # typedplan construction is budget-bounded
        deep = Avro.parseschema("{\"type\":\"record\",\"name\":\"TPB\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        @test Avro.typedplan(@NamedTuple{a::Int64}, deep, Avro.readplan(deep), Avro.Limits()) isa Avro.TypedPlan

        @testset "generated scalar, array and nullable layout oracle" begin
            NestedLayout = @NamedTuple{lead::Int8, inner::@NamedTuple{x::Int64, s::String}, tail::Int16}
            ReferenceLayout = @NamedTuple{s::String, bytes::Vector{UInt8}, items::Vector{String}}
            NullableChild = @NamedTuple{x::Int64, s::String}
            NullableLayout = @NamedTuple{label::Union{Missing,String}, child::Union{Missing,NullableChild}}
            cases = (
                (label="zero-field immutable", T=Empty, value=Empty()),
                (label="zero-field mutable", T=MutEmpty, value=MutEmpty()),
                (label="padded isbits", T=Bits, value=Bits(Int32(3), 1.5f0, true)),
                (label="padded reference-bearing", T=ShellPadded, value=ShellPadded(Int8(1), Int64(2), "pad")),
                (label="mutable reference-bearing", T=ShellMut, value=ShellMut(Int64(3), "mutable")),
                (label="nested inline", T=NestedLayout,
                 value=NestedLayout((Int8(4), (x=Int64(5), s="nested"), Int16(6)))),
                (label="multiple references", T=ReferenceLayout,
                 value=ReferenceLayout(("refs", UInt8[0x01, 0x02, 0x03], String["a", "bb"]))),
                (label="nullable fields", T=NullableLayout,
                 value=NullableLayout((missing, NullableChild((Int64(7), "nullable"))))),
            )

            function chargedtyped(s::Avro.Schema, ::Type{T}, x, names=:trusted) where {T}
                bytes = Avro.encode(s, x)
                target = Avro.typedplan(T, s, Avro.readplan(s), Avro.Limits())
                budget = Avro.Budget(Avro.Limits(); available=1 << 40)
                Avro.addinput!(budget, length(bytes))
                try
                    d = Avro.Decoder(bytes, budget)
                    value = Avro.decodetyped(target, d, names)
                    @test d.pos == length(bytes) + 1
                    return value, budget.reserved
                finally
                    Avro.close!(budget)
                end
            end

            fixed_schema = Avro.FixedSchema("TypedLeafFixed", 4)
            fixed_value = Avro.Fixed(fixed_schema, UInt8[1, 2, 3, 4])
            bytes_value, bytes_charge = chargedtyped(fixed_schema, Vector{UInt8}, fixed_value)
            @test bytes_value == UInt8[1, 2, 3, 4]
            @test bytes_charge == Avro.bytesbytes(4)
            tuple_value, tuple_charge = chargedtyped(fixed_schema, NTuple{4,UInt8}, fixed_value)
            @test tuple_value === (0x01, 0x02, 0x03, 0x04)
            @test tuple_charge == 0
            char_value, char_charge = chargedtyped(Avro.StringSchema(), Char, "é")
            @test char_value === 'é'
            @test char_charge == 0
            for names in (:trusted, Avro.SymbolAdmission(max_names=1))
                symbol_value, symbol_charge = chargedtyped(Avro.StringSchema(), Symbol, "hello", names)
                @test symbol_value === :hello
                @test symbol_charge == 0
            end

            function summarysize(x)
                return Int(Base.summarysize(x; exclude=Avro.Schema))
            end

            function probeshell(::Type{T}) where {T}
                isbitstype(T) && return 0
                return max(Int(Base.summarysize(Avro.emptyprobe(T))), 8)
            end

            for (i, case) in enumerate(cases)
                @testset "$(case.label)" begin
                    T = case.T
                    scalar_schema = Avro.schema(T; name="ShellOracle$i")
                    scalar, scalar_charge = chargedtyped(scalar_schema, T, case.value)
                    @test typeof(scalar) === T
                    if isbitstype(T)
                        @test scalar_charge == 0             # the returned scalar owns no heap storage
                    else
                        @test scalar_charge >= summarysize(scalar)
                    end

                    # A concrete immutable non-isbits record lives inline in Vector{T}. Its exact vector
                    # slots already contain the scalar probe shell; only referenced payload stays extra.
                    array_schema = Avro.ArraySchema(scalar_schema)
                    array, array_charge = chargedtyped(array_schema, Vector{T}, T[case.value, case.value])
                    inline_shell = VERSION >= v"1.11" && isstructtype(T) && !ismutabletype(T) &&
                                   !isbitstype(T) ? probeshell(T) : 0
                    expected_array_charge = Avro.vectorbytes(T, length(array)) +
                                            length(array) * (scalar_charge - inline_shell)
                    @test array_charge == expected_array_charge
                    @test array_charge >= summarysize(array)

                    # A non-isbits nullable vector stores non-missing records through reference slots, so
                    # each present value keeps its scalar shell. Isbits nullable payload stays in the slot.
                    NullableT = Union{Missing,T}
                    nullable_schema = Avro.ArraySchema(Avro.UnionSchema((Avro.NullSchema(), scalar_schema)))
                    nullable_input = Vector{NullableT}(undef, 3)
                    nullable_input[1] = missing
                    nullable_input[2] = case.value
                    nullable_input[3] = case.value
                    nullable, nullable_charge = chargedtyped(nullable_schema, Vector{NullableT}, nullable_input)
                    expected_nullable_charge = Avro.vectorbytes(NullableT, length(nullable)) + 2 * scalar_charge
                    @test nullable_charge == expected_nullable_charge
                    @test nullable_charge >= summarysize(nullable)

                    # A typed map's values vector receives the same immutable inline-shell transfer as
                    # arrays: each decoded value's shell moves into its exact Vector{T} slot (round-3 item 4).
                    map_schema = Avro.MapSchema(scalar_schema)
                    map_input = Avro.Map{T}([("k1", case.value), ("k2", case.value)])
                    avromap, map_charge = chargedtyped(map_schema, Avro.Map{T}, map_input)
                    expected_map_charge = Avro.vectorbytes(String, 2) + 2 * Avro.stringbytes(2) +
                                          Avro.vectorbytes(T, 2) + 2 * (scalar_charge - inline_shell) +
                                          Avro.mapshellbytes(2)
                    @test map_charge == expected_map_charge
                    @test map_charge >= summarysize(avromap)
                end
            end
        end
    end

    @testset "typed map rejects an infinite-minimum value before recursion" begin
        infinite = P("""{"type":"map","values":{"type":"record","name":"InfiniteMapNode","fields":[{"name":"next","type":"InfiniteMapNode"}]}}""")
        err = try
            Avro.DatumReader(infinite, Avro.Map{InfiniteMapNode})(UInt8[0x02, 0x00, 0x00])
            nothing
        catch caught
            caught
        end
        @test err isa Avro.DataError
        @test occursin("no finite datum", sprint(showerror, err))
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
        resolveddefault = Avro.resolve(wdflt, rdflt).plan.defaults[1].second
        defaultbudget = Avro.Budget(Avro.Limits(); available=1 << 40)
        defaultnode = Avro.checkedreaderdefault(Vector{Int64}, resolveddefault, defaultbudget)
        defaultnodecharge = Avro.nodebytes(typeof(defaultnode))
        @test defaultbudget.pending == 0
        @test defaultbudget.reserved == defaultnodecharge                    # validation value is construction scratch
        Avro.release!(defaultbudget, defaultnodecharge)
        Avro.close!(defaultbudget)
        probereader = P("{\"type\":\"record\",\"name\":\"D\",\"fields\":[{\"name\":\"value\",\"type\":\"string\",\"default\":\"x\"}]}")
        probeplan = Avro.resolve(wdflt, probereader).plan.defaults[1].second
        probebudget = Avro.Budget(Avro.Limits(); available=1 << 40)
        @test_throws DefaultProbeError Avro.checkedreaderdefault(
            DefaultProbe, probeplan, probebudget)
        @test probebudget.pending == probebudget.reserved == 0
        Avro.close!(probebudget)

        failedplanbudget = Avro.Budget(Avro.Limits(); available=1 << 40)
        onlya = P("{\"type\":\"record\",\"name\":\"OnlyA\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        @test_throws ArgumentError Avro.typedplan(NoDefault, onlya, Avro.readplan(onlya), Avro.Limits();
            budget=failedplanbudget)
        @test failedplanbudget.pending == failedplanbudget.reserved == 0     # failed construction releases all scratch
        Avro.close!(failedplanbudget)

        directschema = P("{\"type\":\"record\",\"name\":\"DirectCharge\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        directread = Avro.readplan(directschema)
        directbudget = Avro.Budget(Avro.Limits(); available=1 << 40)
        directplan = Avro.typedplan(@NamedTuple{a::Int64}, directschema, directread,
            Avro.Limits(); budget=directbudget)
        directreachable = Avro.nodebytes(typeof(directplan)) + Avro.vectorbytes(Any, length(directplan.defaults))
        @test directbudget.pending == 0
        @test directbudget.reserved == directreachable
        directwork = directbudget.resolution_work
        @test directwork > 0
        Avro.close!(directbudget)
        zerowork = Avro.Limits(max_resolution_work=0)
        zerobudget = Avro.Budget(zerowork; available=1 << 40)
        @test_throws Avro.LimitError Avro.typedplan(@NamedTuple{a::Int64}, directschema, directread,
            zerowork; budget=zerobudget)
        @test zerobudget.pending == zerobudget.reserved == 0
        Avro.close!(zerobudget)
        exactwork = Avro.Limits(max_resolution_work=directwork)
        exactbudget = Avro.Budget(exactwork; available=1 << 40)
        @test Avro.typedplan(@NamedTuple{a::Int64}, directschema, directread,
            exactwork; budget=exactbudget) isa Avro.RecordTarget
        @test exactbudget.resolution_work == directwork
        Avro.close!(exactbudget)
        shortwork = Avro.Limits(max_resolution_work=directwork - 1)
        shortbudget = Avro.Budget(shortwork; available=1 << 40)
        @test_throws Avro.LimitError Avro.typedplan(@NamedTuple{a::Int64}, directschema, directread,
            shortwork; budget=shortbudget)
        Avro.close!(shortbudget)

        resolvedwriter = P("{\"type\":\"record\",\"name\":\"ResolvedCharge\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}")
        resolvedreader = P("{\"type\":\"record\",\"name\":\"ResolvedCharge\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        resolvedplan = Avro.resolve(resolvedwriter, resolvedreader).plan
        resolvedbudget = Avro.Budget(Avro.Limits(); available=1 << 40)
        resolvedtarget = Avro.typedplan(@NamedTuple{a::Int64}, resolvedreader, resolvedplan,
            Avro.Limits(); budget=resolvedbudget)
        resolvedreachable = Avro.nodebytes(typeof(resolvedtarget)) + Avro.vectorbytes(Any, length(resolvedtarget.defaults))
        @test resolvedbudget.pending == 0
        @test resolvedbudget.reserved == resolvedreachable
        Avro.close!(resolvedbudget)

        fallbackwriter = P("{\"type\":\"record\",\"name\":\"FallbackDefaults\",\"fields\":[]}")
        FallbackTarget = @NamedTuple{x::Int32,y::Int64}
        fallbackcharges = Int[]
        for fieldsjson in (
                "[{\"name\":\"x\",\"type\":\"int\",\"default\":1},{\"name\":\"y\",\"type\":\"string\",\"default\":\"a\"}]",
                "[{\"name\":\"y\",\"type\":\"string\",\"default\":\"a\"},{\"name\":\"x\",\"type\":\"int\",\"default\":1}]")
            fallbackreader = P("{\"type\":\"record\",\"name\":\"FallbackDefaults\",\"fields\":" * fieldsjson * "}")
            fallbackplan = Avro.resolve(fallbackwriter, fallbackreader).plan
            fallbackbudget = Avro.Budget(Avro.Limits(); available=1 << 40)
            @test Avro.typedplan(FallbackTarget, fallbackreader, fallbackplan, Avro.Limits();
                budget=fallbackbudget) isa Avro.SemanticTarget
            push!(fallbackcharges, fallbackbudget.reserved)
            Avro.close!(fallbackbudget)
        end
        @test fallbackcharges[1] == fallbackcharges[2]

        enumfallbackschema = P("{\"type\":\"record\",\"name\":\"EnumFallback\",\"fields\":[{\"name\":\"f\",\"type\":{\"type\":\"enum\",\"name\":\"FallbackEnum\",\"symbols\":[\"red\",\"green\",\"blue\"]}},{\"name\":\"bad\",\"type\":\"string\"}]}")
        enumfallbackbudget = Avro.Budget(Avro.Limits(); available=1 << 40)
        enumfallback = Avro.typedplan(EnumFallback, enumfallbackschema, Avro.readplan(enumfallbackschema),
            Avro.Limits(); budget=enumfallbackbudget)
        @test enumfallback isa Avro.SemanticTarget
        @test enumfallbackbudget.pending == 0
        @test enumfallbackbudget.reserved == Avro.nodebytes(typeof(enumfallback))
        Avro.close!(enumfallbackbudget)

        unionfallbackschema = P("[{\"type\":\"enum\",\"name\":\"UnionFallbackEnum\",\"symbols\":[\"red\",\"green\",\"blue\"]},\"string\"]")
        unionfallbackbudget = Avro.Budget(Avro.Limits(); available=1 << 40)
        unionfallback = Avro.typedplan(Union{Colour,Int64}, unionfallbackschema,
            Avro.readplan(unionfallbackschema), Avro.Limits(); budget=unionfallbackbudget)
        @test unionfallback isa Avro.SemanticTarget
        @test unionfallbackbudget.pending == 0
        @test unionfallbackbudget.reserved == Avro.nodebytes(typeof(unionfallback))
        Avro.close!(unionfallbackbudget)

        transactionschema = P("[{\"type\":\"record\",\"name\":\"TransactionOuter\",\"fields\":[{\"name\":\"child\",\"type\":{\"type\":\"record\",\"name\":\"TransactionChild\",\"fields\":[{\"name\":\"f\",\"type\":{\"type\":\"enum\",\"name\":\"TransactionColour\",\"symbols\":[\"red\",\"green\",\"blue\"]}}]}},{\"name\":\"bad\",\"type\":\"string\"}]},\"long\"]")
        transactionplan = Avro.readplan(transactionschema)
        failedfirstbudget = Avro.Budget(Avro.Limits(); available=1 << 40)
        failedfirst = Avro.typedplan(Union{TransactionFail,TransactionGood,Int64}, transactionschema,
            transactionplan, Avro.Limits(); budget=failedfirstbudget)
        successfulbudget = Avro.Budget(Avro.Limits(); available=1 << 40)
        successful = Avro.typedplan(Union{TransactionGood,Int64}, transactionschema,
            transactionplan, Avro.Limits(); budget=successfulbudget)
        @test typeof.(failedfirst.branches) == typeof.(successful.branches)
        @test failedfirstbudget.pending == successfulbudget.pending == 0
        @test failedfirstbudget.reserved == successfulbudget.reserved
        @test failedfirstbudget.resolution_work > successfulbudget.resolution_work
        Avro.close!(failedfirstbudget)
        Avro.close!(successfulbudget)

        for (T, json) in ((Vector{Int64}, "{\"type\":\"array\",\"items\":\"string\"}"),
                          (Dict{String,Int64}, "{\"type\":\"map\",\"values\":\"string\"}"))
            compositeschema = P(json)
            compositebudget = Avro.Budget(Avro.Limits(); available=1 << 40)
            composite = Avro.typedplan(T, compositeschema, Avro.readplan(compositeschema),
                Avro.Limits(); budget=compositebudget)
            @test composite isa Avro.SemanticTarget
            @test compositebudget.pending == 0
            @test compositebudget.reserved == Avro.nodebytes(typeof(composite))
            Avro.close!(compositebudget)
        end

        # a null writer resolves through either nullable reader position and null target convention
        wnull = P("\"null\"")
        for rnull in (P("[\"null\",\"long\"]"), P("[\"long\",\"null\"]"))
            @test Avro.DatumReader(wnull, Union{Nothing,Int64}; reader_schema=rnull)(UInt8[]) === nothing
            @test Avro.DatumReader(wnull, Union{Missing,Int64}; reader_schema=rnull)(UInt8[]) === missing
        end
        # enum remaps stay direct for all supported typed enum representations
        wenum = P("{\"type\":\"enum\",\"name\":\"RemappedColour\",\"symbols\":[\"red\",\"green\",\"blue\"]}")
        renum = P("{\"type\":\"enum\",\"name\":\"RemappedColour\",\"symbols\":[\"blue\",\"green\",\"red\"]}")
        enumbytes = Avro.encode(wenum, "green")
        for (TEnum, expected) in ((String, "green"), (Symbol, :green), (Colour, green))
            enumreader = Avro.DatumReader(wenum, TEnum; reader_schema=renum)
            @test !(enumreader.plan isa Avro.SemanticTarget)
            @test enumreader(enumbytes) === expected
        end
        @test_throws Avro.LimitError Avro.DatumReader(wenum, Symbol; reader_schema=renum,
            names=Avro.SymbolAdmission(max_names=0))(enumbytes)
        # a custom-hooked target still takes the semantic route
        drh = Avro.DatumReader(w, Hooked; reader_schema=r)
        @test drh.plan isa Avro.SemanticTarget || !(drh.plan isa Avro.ResolvedRecordTarget)
        # results agree with the semantic route on the same bytes
        drg = Avro.DatumReader(w; reader_schema=r)
        g = drg(Avro.encode(w, (a=Int32(3), drop="x", b=Int64(7))))
        @test g.a == 3.0 && g.b == 7 && g.z == Int32(9)
    end
end
