module TypedTestTypes
using StructUtils, Dates, UUIDs

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
StructUtils.@kwarg struct MutableDefault
    a::Int64
    extra::Vector{Int} = Int[]
end
StructUtils.@kwarg struct NestedMutableDefault
    a::Int64
    extra::NamedTuple{(:items,),Tuple{Vector{Int}}} = (items=Int[],)
end
const DEFAULT_FACTORY_CALLS = Ref(0)
nextdefault() = (DEFAULT_FACTORY_CALLS[] += 1)
StructUtils.@kwarg struct GeneratedDefault
    a::Int64
    sequence::Int64 = nextdefault()
    id::UUID = uuid4()
end
struct DependentDefault
    a::Int64
    twice::Int64
end
StructUtils.fielddefaults(::StructUtils.StructStyle, ::Type{DependentDefault}, values) =
    (twice=2 * values[1],)
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
struct CheckedSet
    x::Int64
    s::Set{Int64}
    CheckedSet(x::Int64, s::Set{Int64}) = x > 0 ? new(x, s) : throw(ArgumentError("x must be positive"))
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
end

struct Hooked
    a::Float64
    b::Union{Missing,Int64}
    z::Int32
end

function StructUtils.lift(::Avro.AvroStyle, ::Type{Hooked}, x)
    return Hooked(1.0, missing, Int32(0))
end

struct LateHooked                                      # gains a hook only after a first decode
    a::Int64
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

# A hook small enough to inline, which leaves no method of its own in the backtrace.
struct NegativeValue <: Exception
    value::Int64
end
struct CheckedValue
    value::Int64
end
StructUtils.structlike(::Avro.AvroStyle, ::Type{CheckedValue}) = false
StructUtils.lift(::Avro.AvroStyle, ::Type{CheckedValue}, x) = x < 0 ? throw(NegativeValue(x)) : (CheckedValue(x), nothing)

const OVERSIZED_TYPED_FIELD_SOURCE = "x" * repeat("n", 1024)
const OVERSIZED_TYPED_FIELD_NAME = SubString(OVERSIZED_TYPED_FIELD_SOURCE, 2)

function StructUtils.fieldtags(::Avro.AvroStyle, ::Type{OversizedTypedFieldName})
    return (x=(name=OVERSIZED_TYPED_FIELD_NAME,),)
end

struct AnyTags
    tags::Vector
end

struct RecInner{O}
    back::Union{Nothing,O}
end
struct RecOuter                                         # its defaults send it to the semantic route
    x::Int64
    inner::RecInner{RecOuter}
    extra::Union{Nothing,String}
end
StructUtils.fielddefaults(::Avro.AvroStyle, ::Type{RecOuter}) = (extra="dflt",)
struct RecPair
    a::RecOuter
    b::RecInner{RecOuter}
end

@testset "Typed decoding" begin
    using .TypedTestTypes: Plain, Mut, Empty, MutEmpty, Bits, Guarded, Colour, red, green, blue, EnumFallback, TransactionFail, TransactionGood, Tagged, WithDefault, NoDefault, Optional, LL, Node, Shape, Circle, Lifted, Wrapper, HasWrapper, CheckedSet, SymField, Nested
    P = Avro.parseschema
    rs = P("""{"type":"record","name":"R","fields":[
        {"name":"a","type":"int"},{"name":"b","type":"string"},{"name":"c","type":["null","long"]},
        {"name":"d","type":{"type":"array","items":"double"}},{"name":"e","type":{"type":"map","values":"int"}},
        {"name":"f","type":{"type":"enum","name":"E","symbols":["red","green","blue"]}},{"name":"g","type":{"type":"fixed","name":"F","size":2}},
        {"name":"t","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"ch","type":"string"},{"name":"fl","type":"float"}]}""")
    bytes = Avro.encode(rs, (a=1, b="s", c=2, d=[1.5], e=Dict("k" => 3), f="green", g=UInt8[1, 2], t=DateTime(2020), ch="é", fl=0.5f0))
    function plan(T, s=rs)
        return Avro.DatumReader(s, T).node
    end

    @testset "typed targets convert exactly or fail" begin
        ms = P("""{"type":"long","logicalType":"timestamp-millis"}""")
        second = Avro.encode(ms, Avro.timestamp(Millisecond, 1_000))
        @test Avro.decode(ms, second, Avro.Timestamp{Microsecond}) == Avro.timestamp(Microsecond, 1_000_000)
        @test Avro.decode(ms, second, Avro.Timestamp{Nanosecond}) == Avro.timestamp(Nanosecond, 1_000_000_000)
        @test_throws Avro.ConversionError Avro.decode(ms, second, Avro.LocalTimestamp{Millisecond})
        us = P("""{"type":"long","logicalType":"timestamp-micros"}""")
        @test Avro.decode(us, Avro.encode(us, Avro.timestamp(Microsecond, 2_000)), Avro.Timestamp{Millisecond}) ==
              Avro.timestamp(Millisecond, 2)
        @test_throws Avro.ConversionError Avro.decode(us, Avro.encode(us, Avro.timestamp(Microsecond, 1_500)),
                                                      Avro.Timestamp{Millisecond})
        # decoded scalars are not collections
        @test_throws Avro.ConversionError Avro.decode(P("\"string\""), Avro.encode(P("\"string\""), "abc"), Vector{UInt8})
        @test_throws Avro.ConversionError Avro.decode(P("\"long\""), Avro.encode(P("\"long\""), 5), Vector{Int})
        nullschema = Avro.NullSchema()
        for T in (Vector{Any}, Dict{String,Any}, NamedTuple{(),Tuple{}})
            @test_throws Avro.ConversionError Avro.decode(nullschema, UInt8[], T)
            @test_throws Avro.ConversionError Avro.fromjson(nullschema, "null", T)
        end
        @test Avro.decode(nullschema, UInt8[], Union{Missing,Vector{Any}}) === missing
        @test Avro.decode(nullschema, UInt8[], Union{Nothing,Dict{String,Any}}) === nothing
        # a union branch's own Julia type wins; Float16 does not overflow silently
        il = P("""["int","long"]""")
        @test Avro.decode(il, Avro.encode(il, Avro.UnionValue(2, typemax(Int64))), Union{Int32,Int64}) === typemax(Int64)
        fs = P("""["float","string"]""")
        @test_throws Avro.ConversionError Avro.decode(fs, Avro.encode(fs, 1f10), Union{Float16,Float64,String})
        # records decode into dictionary targets
        r = P("""{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}""")
        @test Avro.decode(r, Avro.encode(r, (a=1, b="q")), Dict{String,Any}) == Dict{String,Any}("a" => 1, "b" => "q")
    end

    @testset "logical scalars never become collections of their implementation fields" begin
        cases = (
            (Avro.IntSchema(; logical=Avro.DateLogical()), Date(2026, 1, 2)),
            (Avro.IntSchema(; logical=Avro.TimeMillis()), Time(1, 2, 3)),
            (Avro.LongSchema(; logical=Avro.TimestampMicros()), Avro.timestamp(Microsecond, 1)),
            (Avro.LongSchema(; logical=Avro.LocalTimestampMicros()), Avro.localtimestamp(Microsecond, 1)),
            (Avro.StringSchema(; logical=Avro.UUIDLogical()), UUID("123e4567-e89b-12d3-a456-426614174000")),
            (Avro.FixedSchema("Duration", 12; logical=Avro.DurationLogical()), Avro.Duration(1, 2, 3)),
            (Avro.BytesSchema(; logical=Avro.DecimalLogical(9, 2)), Avro.Decimal(123, 2)),
            (Avro.BytesSchema(; logical=Avro.DecimalLogical(40, 2)), Avro.WideDecimal(big(123), 2)),
        )
        for (schema, value) in cases
            logicalbytes = Avro.encode(schema, value)
            for T in (Vector{Any}, Dict{String,Any}, NamedTuple{(),Tuple{}})
                @test_throws Avro.ConversionError Avro.decode(schema, logicalbytes, T)
                @test_throws Avro.ConversionError Avro.fromjson(schema, Avro.tojson(schema, value), T)
            end
            array = Avro.ArraySchema(schema)
            @test_throws Avro.ConversionError Avro.decode(array, Avro.encode(array, [value]), Vector{Vector{Any}})
            @test Avro.decode(schema, logicalbytes, typeof(value)) == value
        end
    end

    @testset "NamedTuple targets and leaf conversions" begin
        NT = NamedTuple{(:a, :b, :c, :d, :e, :f, :g, :t, :ch, :fl),Tuple{Int,String,Union{Missing,Int},Vector{Float64},Dict{String,Int},Symbol,NTuple{2,UInt8},DateTime,Char,Float64}}
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
        charerror = try
            Avro.decode(P("\"string\""), Avro.encode(P("\"string\""), longchar), Char)
            nothing
        catch caught
            caught
        end
        @test charerror isa Avro.ConversionError
        @test sizeof(charerror.msg) < 256                 # the message does not carry the whole string
        @test Avro.decode(P("\"null\""), UInt8[], Nothing) === nothing
        @test Avro.decode(rs, bytes, Any) == Avro.decode(rs, bytes)
        @test Avro.decode(rs, bytes, Avro.Record) == Avro.decode(rs, bytes) && plan(Avro.Record) isa Avro.GenericNode
        @test plan(Any) isa Avro.GenericNode
        # nullable timestamps to DateTime, a non-optional target rejects null
        ts = P("[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]")
        @test Avro.decode(ts, Avro.encode(ts, DateTime(2021)), Union{Missing,DateTime}) == DateTime(2021)
        @test Avro.decode(ts, Avro.encode(ts, missing), Union{Nothing,DateTime}) === nothing
        @test_throws Avro.ConversionError Avro.decode(ts, Avro.encode(ts, missing), DateTime)
        @test Avro.decode(ts, Avro.encode(ts, DateTime(2021)), DateTime) == DateTime(2021)
        # timestamps out of the DateTime range
        far = Avro.encode(P("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}"), Avro.timestamp(Millisecond, typemax(Int64)))
        @test_throws Avro.ConversionError Avro.decode(P("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}"), far, DateTime)
    end

    @testset "mutable target defaults are fresh for each decoded value" begin
        defaultschema = P("""{"type":"record","name":"D","fields":[{"name":"a","type":"long"}]}""")
        promotedwriter = P("""{"type":"record","name":"D","fields":[{"name":"a","type":"int"}]}""")
        for (T, contents) in ((TypedTestTypes.MutableDefault, x -> x.extra),
                              (TypedTestTypes.NestedMutableDefault, x -> x.extra.items))
            for source in (defaultschema, promotedwriter)
                defaultbytes = Avro.encode(source, (a=1,))
                defaultreader = source === defaultschema ? Avro.DatumReader(source, T) :
                    Avro.DatumReader(source, T; reader_schema=defaultschema)
                @test defaultreader.node isa Avro.SemanticNode
                firstvalue, secondvalue = defaultreader(defaultbytes), defaultreader(defaultbytes)
                @test contents(firstvalue) !== contents(secondvalue)
                push!(contents(firstvalue), 99)
                @test isempty(contents(secondvalue))
                @test isempty(contents(defaultreader(defaultbytes)))
                @test isempty(contents(Avro.decode(source, defaultbytes, T; reader_schema=defaultschema)))
                @test isempty(contents(Avro.fromjson(source, "{\"a\":1}", T)))

                defaultio = IOBuffer()
                Avro.write(defaultio, [(a=1,), (a=2,), (a=3,)]; schema=source)
                defaultrows = collect(Avro.Rows(take!(defaultio), T; reader_schema=defaultschema))
                @test length(defaultrows) == 3
                @test contents(defaultrows[1]) !== contents(defaultrows[2])
                push!(contents(defaultrows[1]), 99)
                @test isempty(contents(defaultrows[2])) && isempty(contents(defaultrows[3]))
            end
        end
        # Implicit optional defaults retain the direct route, including resolution.
        @test Avro.DatumReader(defaultschema, Optional).node isa Avro.RecordNode
        @test Avro.DatumReader(promotedwriter, Optional; reader_schema=defaultschema).node isa Avro.RecordNode
        for source in (defaultschema, promotedwriter)
            TypedTestTypes.DEFAULT_FACTORY_CALLS[] = 0
            factoryreader = Avro.DatumReader(source, TypedTestTypes.GeneratedDefault; reader_schema=defaultschema)
            @test TypedTestTypes.DEFAULT_FACTORY_CALLS[] == 0
            factorybytes = Avro.encode(source, (a=3,))
            firstvalue, secondvalue = factoryreader(factorybytes), factoryreader(factorybytes)
            @test (firstvalue.sequence, secondvalue.sequence) == (1, 2)
            @test firstvalue.id != secondvalue.id
            @test TypedTestTypes.DEFAULT_FACTORY_CALLS[] == 2
            oneshot = Avro.decode(source, factorybytes, TypedTestTypes.GeneratedDefault; reader_schema=defaultschema)
            jsonvalue = Avro.fromjson(source, "{\"a\":3}", TypedTestTypes.GeneratedDefault)
            @test (oneshot.sequence, jsonvalue.sequence) == (3, 4)
            @test TypedTestTypes.DEFAULT_FACTORY_CALLS[] == 4
            factoryio = IOBuffer()
            Avro.write(factoryio, [(a=1,), (a=2,)]; schema=source)
            factoryrows = Avro.Rows(take!(factoryio), TypedTestTypes.GeneratedDefault; reader_schema=defaultschema)
            @test TypedTestTypes.DEFAULT_FACTORY_CALLS[] == 4
            rowvalues = collect(factoryrows)
            @test getfield.(rowvalues, :sequence) == [5, 6]
            @test TypedTestTypes.DEFAULT_FACTORY_CALLS[] == 6
            @test rowvalues[1].id != rowvalues[2].id
            dependent = Avro.DatumReader(source, TypedTestTypes.DependentDefault; reader_schema=defaultschema)
            @test dependent(factorybytes).twice == 6
        end
        fullschema = P("""{"type":"record","name":"Full","fields":[
            {"name":"a","type":"long"},{"name":"sequence","type":"long"},
            {"name":"id","type":{"type":"string","logicalType":"uuid"}}]}""")
        TypedTestTypes.DEFAULT_FACTORY_CALLS[] = 0
        fullreader = Avro.DatumReader(fullschema, TypedTestTypes.GeneratedDefault)
        @test fullreader.node isa Avro.RecordNode
        @test TypedTestTypes.DEFAULT_FACTORY_CALLS[] == 0
        id = UUID("123e4567-e89b-12d3-a456-426614174000")
        @test fullreader(Avro.encode(fullschema, (a=1, sequence=7, id=id))).sequence == 7
        @test TypedTestTypes.DEFAULT_FACTORY_CALLS[] == 0
    end

    @testset "struct targets: the fast route builds through the positional constructor" begin
        ps = P("{\"type\":\"record\",\"name\":\"Plain\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"string\"}]}")
        pb = Avro.encode(ps, Plain(1, "x"))
        @test Avro.decode(ps, pb, Plain) == Plain(1, "x") && plan(Plain, ps) isa Avro.RecordNode
        gs = P("{\"type\":\"record\",\"name\":\"Guarded\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        @test_throws ArgumentError Guarded(-5)
        @test_throws ArgumentError Avro.decode(gs, Avro.encode(gs, (a=-5,)), Guarded)   # its validation runs
        @test Avro.decode(gs, Avro.encode(gs, (a=5,)), Guarded).a == 5
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

        # field name tags, defaults, optional fields, missing defaults
        ts = P("{\"type\":\"record\",\"name\":\"T\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"string\"}]}")
        @test Avro.decode(ts, Avro.encode(ts, (a=1, b="x")), Tagged) == Tagged(1, "x")
        @test plan(Tagged, ts) isa Avro.RecordNode
        as = P("{\"type\":\"record\",\"name\":\"A\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        @test_throws Avro.ConversionError Avro.DatumReader(as, OversizedTypedFieldName)   # a 1024-byte target field name the schema lacks
        wd = Avro.decode(as, Avro.encode(as, (a=9,)), WithDefault)
        @test wd.a == 9 && wd.extra == "dflt" && wd.opt === missing
        @test_throws Avro.ConversionError Avro.DatumReader(as, NoDefault)
        @test Avro.decode(as, Avro.encode(as, (a=9,)), Optional) == Optional(9, nothing)
        # schema fields absent from the target are skipped: a sized block is jumped by its size
        xs = P("{\"type\":\"record\",\"name\":\"X\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"junk\",\"type\":{\"type\":\"array\",\"items\":\"boolean\"}},{\"name\":\"b\",\"type\":\"string\"}]}")
        xb = Avro.encode(xs, (a=1, junk=[true], b="x"))
        @test Avro.decode(xs, xb, Plain) == Plain(1, "x")
        bad = vcat(Avro.encode(P("\"long\""), 1), hex2bytes("01" * "02" * "02" * "00"), Avro.encode(P("\"string\""), "x"))  # sized block with a bad boolean
        @test Avro.decode(xs, bad, Plain) == Plain(1, "x")
        @test_throws Avro.DataError Avro.decode(xs, bad)                 # the generic decode reads the block
    end

    @testset "recursion and Julia unions" begin
        ll = P("{\"type\":\"record\",\"name\":\"LL\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"},{\"name\":\"next\",\"type\":[\"null\",\"LL\"]}]}")
        v = LL(1, LL(2, LL(3, missing)))
        @test Avro.decode(ll, Avro.encode(ll, v), LL) == v
        @test plan(LL, ll) isa Avro.RecordNode
        @test Avro.decode(P("[\"int\",\"string\"]"), UInt8[0x00, 0x00], Union{Int32,String}) === Int32(0)
        @test Avro.decode(P("[\"null\",\"int\"]"), UInt8[0x00], Union{Missing,Int32}) === missing
        @test Avro.decode(P("\"null\""), UInt8[], Union{Missing,Nothing,Int64}) === missing     # several non-null members
        @test Avro.decode(P("\"null\""), UInt8[], Union{Missing,Int32,String}) === missing
        tree = P("{\"type\":\"record\",\"name\":\"Node\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"children\",\"type\":{\"type\":\"array\",\"items\":\"Node\"}}]}")
        n = Node("r", [Node("a", Node[]), Node("b", [Node("c", Node[])])])
        @test Avro.decode(tree, Avro.encode(tree, n), Node) == n
        u = P("[\"int\",\"string\",\"null\"]")
        @test Avro.decode(u, Avro.encode(u, Int32(3)), Union{Int32,String,Nothing}) === Int32(3)
        @test Avro.decode(u, Avro.encode(u, "s"), Union{Int32,String,Nothing}) == "s"
        @test Avro.decode(u, Avro.encode(u, missing), Union{Int32,String,Nothing}) === nothing
        @test Avro.decode(u, Avro.encode(u, Int32(3)), Union{Int64,String,Missing}) === Int64(3)
        @test plan(Union{Int32,String,Nothing}, u) isa Avro.UnionNode
        @test plan(Union{Int32,Nothing}, u) isa Avro.SemanticNode          # no member for the string branch
        @test_throws Avro.ConversionError Avro.decode(u, Avro.encode(u, "s"), Union{Int32,Nothing})
        @test Avro.decode(u, Avro.encode(u, Int32(3)), Union{Int32,Nothing}) === Int32(3)   # semantic route succeeds when possible
        arr = P("{\"type\":\"array\",\"items\":[\"null\",\"int\"]}")
        @test Avro.decode(arr, Avro.encode(arr, [1, missing]), Vector{Union{Nothing,Int}}) == [1, nothing]
        @test isequal(Avro.decode(arr, Avro.encode(arr, [1, missing]), Vector{Any}), [Int32(1), missing])
        @test isequal(Avro.decode(arr, Avro.encode(arr, [1, missing]), Vector{Union{Missing,Int}}), [1, missing])
        # a record on the semantic route leaves no half-built node behind for the nodes that refer back to it
        w = P("""{"type":"record","name":"W","fields":[
            {"name":"a","type":{"type":"record","name":"Outer","fields":[{"name":"x","type":"long"},
                {"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"back","type":["null","Outer"]}]}}]}},
            {"name":"b","type":"Inner"}]}""")
        pair = Avro.decode(w, Avro.encode(w, (a=(x=1, inner=(back=nothing,)), b=(back=(x=2, inner=(back=nothing,)),))), RecPair)
        @test pair == RecPair(RecOuter(1, RecInner{RecOuter}(nothing), "dflt"), RecInner{RecOuter}(RecOuter(2, RecInner{RecOuter}(nothing), "dflt")))
    end

    @testset "abstract vector targets convert on the semantic route" begin
        tags = P("""{"type":"record","name":"AnyTags","fields":[{"name":"tags","type":{"type":"array","items":"string"}}]}""")
        @test Avro.decode(tags, Avro.encode(tags, (tags=["a"],)), AnyTags).tags == Any["a"]
        nested = P("""{"type":"array","items":{"type":"array","items":"string"}}""")
        @test Avro.decode(nested, Avro.encode(nested, [["a"]]), Vector{Vector}) == [Any["a"]]
        m = P("""{"type":"map","values":{"type":"array","items":"string"}}""")
        @test Avro.decode(m, Avro.encode(m, Dict("k" => ["a"])), Dict{String,Vector}) == Dict("k" => Any["a"])
    end

    @testset "semantic route and custom hooks" begin
        hs = P("{\"type\":\"record\",\"name\":\"Circle\",\"fields\":[{\"name\":\"r\",\"type\":\"double\"}]}")
        @test plan(Shape, hs) isa Avro.SemanticNode && plan(Circle, hs) isa Avro.RecordNode
        @test Avro.decode(hs, Avro.encode(hs, (r=4.0,)), Shape) === Circle(4.0)
        ls = P("{\"type\":\"record\",\"name\":\"Lifted\",\"fields\":[{\"name\":\"when\",\"type\":\"string\"}]}")
        @test plan(Lifted, ls) isa Avro.SemanticNode                       # a dateformat tag affects construction
        @test Avro.decode(ls, Avro.encode(ls, (when="2020-02-03",)), Lifted) == Lifted(Date(2020, 2, 3))
        ws = P("{\"type\":\"record\",\"name\":\"HasWrapper\",\"fields\":[{\"name\":\"w\",\"type\":\"string\"}]}")
        @test plan(HasWrapper, ws) isa Avro.RecordNode                     # only the field converts through its lift
        @test Avro.decode(ws, Avro.encode(ws, (w="x",)), HasWrapper) == HasWrapper(Wrapper("w:x"))
        # a field StructUtils converts keeps its record on the direct route, so the constructor's own error propagates
        cs = P("{\"type\":\"record\",\"name\":\"CheckedSet\",\"fields\":[{\"name\":\"x\",\"type\":\"long\"},{\"name\":\"s\",\"type\":{\"type\":\"array\",\"items\":\"long\"}}]}")
        @test plan(CheckedSet, cs) isa Avro.RecordNode
        @test Avro.decode(cs, Avro.encode(cs, (x=2, s=[1, 1])), CheckedSet).s == Set([1])
        @test_throws ArgumentError Avro.decode(cs, Avro.encode(cs, (x=-1, s=[1])), CheckedSet)
        # arraylike / dictlike targets outside the fast route
        arr = P("{\"type\":\"array\",\"items\":\"int\"}")
        @test Avro.decode(arr, Avro.encode(arr, [1, 2, 2]), Set{Int}) == Set([1, 2])
        @test Avro.decode(arr, Avro.encode(arr, [1, 2]), NTuple{2,Int}) === (1, 2)
        mp = P("{\"type\":\"map\",\"values\":\"int\"}")
        @test Avro.decode(mp, Avro.encode(mp, Dict("a" => 1)), Dict{String,Any}) == Dict("a" => 1)
        @test Avro.decode(mp, Avro.encode(mp, Dict("a" => 1)), Dict{String,Int64}) == Dict{String,Int64}("a" => 1)
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
        longschema = P("\"long\"")
        @test Avro.decode(longschema, Avro.encode(longschema, 5), CheckedValue) == CheckedValue(5)
        @test_throws NegativeValue(-5) Avro.decode(longschema, Avro.encode(longschema, -5), CheckedValue)
        # a conversion failure says what Base raised
        @test_throws r"to Set\{Int8\}: \"InexactError: trunc\(Int8, 300\)\"" Avro.decode(P("{\"type\":\"array\",\"items\":\"long\"}"),
                                                                                    Avro.encode(P("{\"type\":\"array\",\"items\":\"long\"}"), [300]), Set{Int8})

        # hook answers are cached per world: a hook defined after a first decode is still honored
        late = P("{\"type\":\"record\",\"name\":\"LateHooked\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        latebytes = Avro.encode(late, (a=5,))
        @test Avro.decode(late, latebytes, LateHooked) == LateHooked(5)
        @test Avro.DatumReader(late, LateHooked).node isa Avro.RecordNode
        @test haskey(Avro.HOOK_CACHE.answers, LateHooked)
        @eval StructUtils.lift(::Avro.AvroStyle, ::Type{LateHooked}, x) = LateHooked(-1)
        @test Base.invokelatest(Avro.DatumReader, late, LateHooked).node isa Avro.SemanticNode
        for i in 1:(Avro.HOOK_CACHE_SIZE + 1)          # the cache stays bounded
            Avro.customhooks(NamedTuple{(Symbol("hook", i),),Tuple{Int}})
        end
        @test length(Avro.HOOK_CACHE.answers) <= Avro.HOOK_CACHE_SIZE
    end

    @testset "Symbol targets on every typed path" begin
        ss = P("{\"type\":\"record\",\"name\":\"S\",\"fields\":[{\"name\":\"s\",\"type\":\"string\"},{\"name\":\"e\",\"type\":{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"p\",\"q\"]}}]}")
        sb = Avro.encode(ss, (s="hello", e="q"))
        @test Avro.decode(ss, sb, SymField) == SymField(:hello, :q) && plan(SymField, ss) isa Avro.RecordNode
        # map keys and the semantic route
        mp = P("{\"type\":\"map\",\"values\":\"int\"}")
        mb = Avro.encode(mp, Dict("a" => 1, "b" => 2))
        @test Avro.decode(mp, mb, Dict{Symbol,Int}) == Dict(:a => 1, :b => 2)
        @test Avro.decode(mp, mb, Dict{Symbol,Any}) == Dict{Symbol,Any}(:a => 1, :b => 2)
        sym = P("\"string\"")
        @test Avro.decode(sym, Avro.encode(sym, "zz"), Symbol) === :zz
        arr = P("{\"type\":\"array\",\"items\":\"string\"}")
        @test Avro.decode(arr, Avro.encode(arr, ["a", "b"]), Vector{Symbol}) == [:a, :b]
        @test Avro.decode(arr, Avro.encode(arr, ["a", "b"]), Set{Symbol}) == Set([:a, :b])
    end

    @testset "allocation-free typed and generic kernels; prepared readers" begin
        bs = P("{\"type\":\"record\",\"name\":\"Bits\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"f\",\"type\":\"float\"},{\"name\":\"flag\",\"type\":\"boolean\"}]}")
        reader = Avro.DatumReader(bs, Bits)
        bb = Avro.encode(bs, Bits(3, 1.5f0, true))
        @test reader(bb) === Bits(3, 1.5f0, true)
        d = Avro.Decoder(bb)
        function kernel(reader, d)
            Avro.reset!(d, d.buf, 1)
            return Avro.decodenode(Bits, reader.node, d)
        end

        function measure(reader, d)
            kernel(reader, d)
            return @allocated(kernel(reader, d))
        end
        @test measure(reader, d) == 0
        ls = P("\"long\"")
        lr = Avro.DatumReader(ls)
        lb = Avro.encode(ls, 7)
        d2 = Avro.Decoder(lb)
        function gk(plan, d)
            Avro.reset!(d, d.buf, 1)
            return Avro.decode(plan, d)
        end

        function gmeasure(plan, d)
            gk(plan, d)
            return @allocated(gk(plan, d))
        end
        @test gmeasure(lr.node.plan, d2) == 0
        tasks = [Threads.@spawn reader(bb) for _ in 1:4]
        foreach(errormonitor, tasks)
        @test all(fetch(t) === Bits(3, 1.5f0, true) for t in tasks)
        @test Avro.DatumReader(bs, Bits)(IOBuffer(bb)) === Bits(3, 1.5f0, true)
        @test Avro.DatumReader(bs, Bits)(vcat(bb, bb), length(bb) + 1) == (Bits(3, 1.5f0, true), 2 * length(bb) + 1)
    end

    @testset "typed layouts: scalars, arrays, nullables and maps" begin
        deep = Avro.parseschema("{\"type\":\"record\",\"name\":\"TPB\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        @test Avro.typednode(@NamedTuple{a::Int64}, deep, Avro.readplan(deep)) isa Avro.TypedNode
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

        function typed(s::Avro.Schema, ::Type{T}, x) where {T}
            bytes = Avro.encode(s, x)
            target = Avro.typednode(T, s, Avro.readplan(s))
            d = Avro.Decoder(bytes)
            value = Avro.decodenode(T, target, d)
            @test d.pos == length(bytes) + 1
            return value
        end

        fixed_schema = Avro.FixedSchema("TypedLeafFixed", 4)
        fixed_value = Avro.Fixed(fixed_schema, UInt8[1, 2, 3, 4])
        @test typed(fixed_schema, Vector{UInt8}, fixed_value) == UInt8[1, 2, 3, 4]
        @test typed(fixed_schema, NTuple{4,UInt8}, fixed_value) === (0x01, 0x02, 0x03, 0x04)
        @test typed(Avro.StringSchema(), Char, "é") === 'é'
        @test typed(Avro.StringSchema(), Symbol, "hello") === :hello
        for (i, case) in enumerate(cases)
            @testset "$(case.label)" begin
                T = case.T
                scalar_schema = Avro.schema(T; name="ShellOracle$i")
                @test typeof(typed(scalar_schema, T, case.value)) === T
                array = typed(Avro.ArraySchema(scalar_schema), Vector{T}, T[case.value, case.value])
                @test array isa Vector{T} && length(array) == 2
                NullableT = Union{Missing,T}
                nullable_schema = Avro.ArraySchema(Avro.UnionSchema((Avro.NullSchema(), scalar_schema)))
                nullable_input = Vector{NullableT}(undef, 3)
                nullable_input[1] = missing
                nullable_input[2] = case.value
                nullable_input[3] = case.value
                nullable = typed(nullable_schema, Vector{NullableT}, nullable_input)
                @test nullable isa Vector{NullableT} && ismissing(nullable[1]) && nullable[2] isa T && nullable[3] isa T
                map_input = Dict{String,T}("k1" => case.value, "k2" => case.value)
                typedmap = typed(Avro.MapSchema(scalar_schema), Dict{String,T}, map_input)
                @test typedmap isa Dict{String,T} && sort!(collect(keys(typedmap))) == ["k1", "k2"]
            end
        end
    end

    @testset "typed map rejects an infinite-minimum value before recursion" begin
        infinite = P("""{"type":"map","values":{"type":"record","name":"InfiniteMapNode","fields":[{"name":"next","type":"InfiniteMapNode"}]}}""")
        err = try
            Avro.DatumReader(infinite, Dict{String,InfiniteMapNode})(UInt8[0x02, 0x00, 0x00])
            nothing
        catch caught
            caught
        end
        @test err isa Avro.DataError
        @test occursin("no finite datum", sprint(showerror, err))
    end

    @testset "the direct typed route over resolving plans" begin
        w = P("{\"type\":\"record\",\"name\":\"E1\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"drop\",\"type\":\"string\"},{\"name\":\"b\",\"type\":[\"null\",\"long\"]}]}")
        r = P("{\"type\":\"record\",\"name\":\"E1\",\"fields\":[{\"name\":\"a\",\"type\":\"double\"},{\"name\":\"b\",\"type\":[\"null\",\"long\"]},{\"name\":\"z\",\"type\":\"int\",\"default\":9}]}")
        T = @NamedTuple{a::Float64, b::Union{Missing,Int64}, z::Int32}
        dr = Avro.DatumReader(w, T; reader_schema=r)
        @test dr.node isa Avro.RecordNode                              # not the semantic fallback
        @test dr(Avro.encode(w, (a=Int32(3), drop="x", b=Int64(7)))) == (a=3.0, b=7, z=Int32(9))
        @test isequal(dr(Avro.encode(w, (a=Int32(1), drop="y", b=missing))), (a=1.0, b=missing, z=Int32(9)))
        # an immutable struct target
        drs = Avro.DatumReader(w, ResolvedDTO; reader_schema=r)
        @test drs.node isa Avro.RecordNode
        v = drs(Avro.encode(w, (a=Int32(2), drop="q", b=Int64(5))))
        @test v.a == 2.0 && v.b == 5 && v.z == Int32(9)
        # nested resolved records recurse onto the direct route
        wn = P("{\"type\":\"record\",\"name\":\"O\",\"fields\":[{\"name\":\"in\",\"type\":{\"type\":\"record\",\"name\":\"I\",\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}}]}")
        rn = P("{\"type\":\"record\",\"name\":\"O\",\"fields\":[{\"name\":\"in\",\"type\":{\"type\":\"record\",\"name\":\"I\",\"fields\":[{\"name\":\"x\",\"type\":\"long\"}]}}]}")
        TN = @NamedTuple{in::@NamedTuple{x::Int64}}
        drn = Avro.DatumReader(wn, TN; reader_schema=rn)
        @test drn.node isa Avro.RecordNode
        @test drn(Avro.encode(wn, (in=(x=Int32(4),),))) == (in=(x=Int64(4),),)
        # recursive resolved records terminate plan construction and stay on the direct route
        wll = P("{\"type\":\"record\",\"name\":\"RLL\",\"fields\":[{\"name\":\"value\",\"type\":\"int\"},{\"name\":\"next\",\"type\":[\"null\",\"RLL\"]}]}")
        rll = P("{\"type\":\"record\",\"name\":\"RLL\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"},{\"name\":\"next\",\"type\":[\"null\",\"RLL\"]}]}")
        drll = Avro.DatumReader(wll, LL; reader_schema=rll)
        @test drll.node isa Avro.RecordNode
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
        # defaults are the caller's values, so they do not count against the file's value cap
        tiny = Avro.Limits(value_allowance=1)
        @test Avro.DatumReader(wdflt; reader_schema=rdflt, limits=tiny)(UInt8[]).xs == [1]
        @test Avro.DatumReader(wdflt, TD; reader_schema=rdflt, limits=tiny)(UInt8[]).xs == [1]
        # a reader's fixed default fills only a byte tuple of the fixed's size
        rfix = P("{\"type\":\"record\",\"name\":\"D\",\"fields\":[{\"name\":\"f\",\"type\":{\"type\":\"fixed\",\"name\":\"F2\",\"size\":2},\"default\":\"ab\"}]}")
        @test Avro.decode(wdflt, UInt8[], @NamedTuple{f::NTuple{2,UInt8}}; reader_schema=rfix) == (f=(0x61, 0x62),)
        @test_throws Avro.ConversionError Avro.decode(wdflt, UInt8[], @NamedTuple{f::NTuple{8,UInt8}}; reader_schema=rfix)
        @test_throws Avro.ConversionError Avro.decode(wdflt, UInt8[], @NamedTuple{f::NTuple{1,UInt8}}; reader_schema=rfix)
        # and a fixed value converts only to a byte tuple of its size on every route
        fx = P("{\"type\":\"record\",\"name\":\"FX\",\"fields\":[{\"name\":\"f\",\"type\":{\"type\":\"fixed\",\"name\":\"F3\",\"size\":2}}]}")
        fxbytes = Avro.encode(fx, (f=(0x61, 0x62),))
        @test Avro.decode(fx, fxbytes, @NamedTuple{f::NTuple{2,UInt8}}) == (f=(0x61, 0x62),)
        @test_throws Avro.ConversionError Avro.decode(fx, fxbytes, @NamedTuple{f::NTuple{1,UInt8}})
        @test_throws Avro.ConversionError Avro.decode(fx.fields[1].schema, UInt8[0x61, 0x62], NTuple{1,UInt8})

        onlya = P("{\"type\":\"record\",\"name\":\"OnlyA\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        @test_throws Avro.ConversionError Avro.typednode(NoDefault, onlya, Avro.readplan(onlya))

        directschema = P("{\"type\":\"record\",\"name\":\"DirectCharge\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        @test Avro.typednode(@NamedTuple{a::Int64}, directschema, Avro.readplan(directschema)) isa Avro.RecordNode
        resolvedwriter = P("{\"type\":\"record\",\"name\":\"ResolvedCharge\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}")
        resolvedreader = P("{\"type\":\"record\",\"name\":\"ResolvedCharge\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        resolvedplan = Avro.resolve(resolvedwriter, resolvedreader).plan
        @test Avro.typednode(@NamedTuple{a::Int64}, resolvedreader, resolvedplan) isa Avro.RecordNode

        fallbackwriter = P("{\"type\":\"record\",\"name\":\"FallbackDefaults\",\"fields\":[]}")
        FallbackTarget = @NamedTuple{x::Int32,y::Int64}
        for fieldsjson in (
                "[{\"name\":\"x\",\"type\":\"int\",\"default\":1},{\"name\":\"y\",\"type\":\"string\",\"default\":\"a\"}]",
                "[{\"name\":\"y\",\"type\":\"string\",\"default\":\"a\"},{\"name\":\"x\",\"type\":\"int\",\"default\":1}]")
            fallbackreader = P("{\"type\":\"record\",\"name\":\"FallbackDefaults\",\"fields\":" * fieldsjson * "}")
            fallbackplan = Avro.resolve(fallbackwriter, fallbackreader).plan
            @test Avro.typednode(FallbackTarget, fallbackreader, fallbackplan) isa Avro.SemanticNode
        end

        enumfallbackschema = P("{\"type\":\"record\",\"name\":\"EnumFallback\",\"fields\":[{\"name\":\"f\",\"type\":{\"type\":\"enum\",\"name\":\"FallbackEnum\",\"symbols\":[\"red\",\"green\",\"blue\"]}},{\"name\":\"bad\",\"type\":\"string\"}]}")
        enumfallbackplan = Avro.typednode(EnumFallback, enumfallbackschema, Avro.readplan(enumfallbackschema))
        @test enumfallbackplan isa Avro.RecordNode && !Avro.isdirect(enumfallbackplan)   # `bad` converts through StructUtils
        @test_throws Avro.ConversionError Avro.decode(enumfallbackschema, Avro.encode(enumfallbackschema, (f="red", bad="7")), EnumFallback)
        unionfallbackschema = P("[{\"type\":\"enum\",\"name\":\"UnionFallbackEnum\",\"symbols\":[\"red\",\"green\",\"blue\"]},\"string\"]")
        @test Avro.typednode(Union{Colour,Int64}, unionfallbackschema, Avro.readplan(unionfallbackschema)) isa Avro.SemanticNode

        transactionschema = P("[{\"type\":\"record\",\"name\":\"TransactionOuter\",\"fields\":[{\"name\":\"child\",\"type\":{\"type\":\"record\",\"name\":\"TransactionChild\",\"fields\":[{\"name\":\"f\",\"type\":{\"type\":\"enum\",\"name\":\"TransactionColour\",\"symbols\":[\"red\",\"green\",\"blue\"]}}]}},{\"name\":\"bad\",\"type\":\"string\"}]},\"long\"]")
        transactionplan = Avro.readplan(transactionschema)
        failedfirst = Avro.typednode(Union{TransactionFail,TransactionGood,Int64}, transactionschema, transactionplan)
        successful = Avro.typednode(Union{TransactionGood,Int64}, transactionschema, transactionplan)
        @test typeof.(failedfirst.branches) == typeof.(successful.branches)

        for (T, json) in ((Vector{Int64}, "{\"type\":\"array\",\"items\":\"string\"}"),
                          (Dict{String,Int64}, "{\"type\":\"map\",\"values\":\"string\"}"))
            compositeschema = P(json)
            @test Avro.typednode(T, compositeschema, Avro.readplan(compositeschema)) isa Avro.SemanticNode
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
            @test !(enumreader.node isa Avro.SemanticNode)
            @test enumreader(enumbytes) === expected
        end
        # a custom-hooked target still takes the semantic route
        drh = Avro.DatumReader(w, Hooked; reader_schema=r)
        @test drh.node isa Avro.SemanticNode || !(drh.node isa Avro.RecordNode)
        # results agree with the semantic route on the same bytes
        drg = Avro.DatumReader(w; reader_schema=r)
        g = drg(Avro.encode(w, (a=Int32(3), drop="x", b=Int64(7))))
        @test g.a == 3.0 && g.b == 7 && g.z == Int32(9)
    end
end
