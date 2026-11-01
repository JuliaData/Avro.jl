# The value model and input forms users meet first, and how they work with other packages.
import Arrow, CSV, DataAPI, DataFrames, JSON, TimeZones
using TimeZones: ZonedDateTime, @tz_str
using UUIDs: uuid4

struct InterfacePerson
    id::Int64
    name::String
end

struct InterfaceWrongTypes
    id::String
    name::String
end

struct InterfaceReversed
    name::String
    id::Int64
end

@enum InterfaceColor interface_red interface_green

struct CountingStore <: Avro.SchemaStore
    store::Avro.SchemaCache
    lookups::Base.RefValue{Int}
end

function Avro.lookup(c::CountingStore, fp::UInt64)
    c.lookups[] += 1
    return Avro.lookup(c.store, fp)
end

struct LyingStore <: Avro.SchemaStore
    schema::Avro.Schema
end

Avro.lookup(c::LyingStore, ::UInt64) = c.schema

@testset "Interfaces" begin
    P = Avro.parseschema
    person = P("""{"type":"record","name":"Person","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}""")

    @testset "an enum value is its symbol's string" begin
        e = P("""{"type":"enum","name":"E","symbols":["A","B"]}""")
        x = Avro.EnumValue(e, "B")
        @test x isa AbstractString && x == "B" && "B" == x && x != "A"
        @test hash(x) == hash("B") && Dict("B" => 1)[x] == 1 && isequal(x, "B")
        @test "$x" == "B" && String(x) == "B" && Symbol(x) === :B && length(x) == 1 && lowercase(x) == "b" && x * "!" == "B!"
        @test x == Avro.EnumValue(P(Avro.json(e)), 2) && Avro.schema(x) === e && Avro.ordinal(x) == 1
        @test sprint(show, x) == "Avro.EnumValue(E.B)"
        @test JSON.json(x) == "\"B\""
        @test Avro.schema(Avro.EnumValue) isa Avro.StringSchema
        @test Avro.decode(Avro.StringSchema(), Avro.encode(Avro.StringSchema(), x)) == "B"
        u = P("""["string",{"type":"enum","name":"E","symbols":["A","B"]}]""")
        @test Avro.encode(u, Avro.EnumValue(u.branches[2], "A"))[1] == 0x02     # its identity picks the enum branch
        @test Avro.encode(u, "A")[1] == 0x00                                    # a plain string is a string
        @test Avro.encode(P("""["long","string"]"""), x)[1] == 0x02             # no such identity: it is a string
    end

    @testset "a fixed value is its bytes" begin
        f = Avro.FixedSchema("F", 2)
        x = Avro.Fixed(f, UInt8[1, 2])
        @test x isa AbstractVector{UInt8} && x == UInt8[1, 2] && UInt8[1, 2] == x && length(x) == 2 && x[2] == 0x02
        @test hash(x) == hash(UInt8[1, 2]) && bytes2hex(x) == "0102" && Vector{UInt8}(x) == UInt8[1, 2]
        @test x == Avro.Fixed(Avro.FixedSchema("G", 2), UInt8[1, 2]) && Avro.schema(x) === f
        @test sprint(show, x) == "Avro.Fixed(F: 0x0102)" && sprint(show, MIME"text/plain"(), x) == "Avro.Fixed(F: 0x0102)"
        @test JSON.json(x) == "[1,2]"
        @test Avro.schema(Avro.Fixed) isa Avro.BytesSchema && Avro.schema(typeof(view(UInt8[1], 1:1))) isa Avro.BytesSchema
        @test Avro.decode(Avro.BytesSchema(), Avro.encode(Avro.BytesSchema(), x)) == UInt8[1, 2]
        u = P("""["bytes",{"type":"fixed","name":"F","size":2}]""")
        @test Avro.encode(u, Avro.Fixed(u.branches[2], UInt8[1, 2])) == UInt8[0x02, 0x01, 0x02]
        @test Avro.encode(u, UInt8[1, 2])[1] == 0x00
        @test Avro.encode(P("""["long","bytes"]"""), x)[1] == 0x02
    end

    @testset "records" begin
        r = Avro.decode(person, Avro.encode(person, (id=1, name="a")))
        @test NamedTuple(r) === (id=Int64(1), name="a")
        @test JSON.json(r) == "{\"id\":1,\"name\":\"a\"}"
        rich = P("""{"type":"record","name":"R","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["A"]}},
                     {"name":"f","type":{"type":"fixed","name":"F","size":1}},{"name":"o","type":["null","long"]}]}""")
        value = Avro.decode(rich, Avro.encode(rich, (e="A", f=UInt8[7], o=missing)))
        @test JSON.json(value) == "{\"e\":\"A\",\"f\":[7],\"o\":null}"
    end

    @testset "Avro.write takes collections of datums" begin
        people = [InterfacePerson(1, "a"), InterfacePerson(2, "b")]
        buf = Avro.tobuffer(people)
        t = Avro.Table(buf)
        @test Avro.fullname(Avro.schema(t)) == "Main.InterfacePerson" || endswith(Avro.fullname(Avro.schema(t)), ".InterfacePerson")
        @test t.id == [1, 2] && t.name == ["a", "b"]
        @test collect(Avro.Rows(seekstart(buf); T=InterfacePerson)) == people
        @test Avro.fullname(Avro.schema(Avro.Table(Avro.tobuffer(people; name="Q", namespace="n")))) == "n.Q"
        @test Avro.Table(Avro.tobuffer(people; schema=person)).id == [1, 2]
        @test length(Avro.Table(Avro.tobuffer(InterfacePerson[]))) == 0
        @test Avro.Table(Avro.tobuffer(InterfacePerson(i, "x") for i in 1:3)).id == 1:3
        # the same bytes through the aligned path, by name, and from a NamedTuple
        rows = [(id=p.id, name=p.name) for p in people]
        sync = zeros(UInt8, 16)
        same = take!(Avro.tobuffer(rows; schema=person, sync=sync))
        @test take!(Avro.tobuffer(people; schema=person, sync=sync)) == same
        @test take!(Avro.tobuffer([InterfaceReversed(p.name, p.id) for p in people]; schema=person, sync=sync)) == same
        # failures name the value's path, whichever path encoded it
        for writeone in (w -> push!(w, InterfaceWrongTypes("1", "a")),
                         w -> Avro.pushrowsof!(w, i -> InterfaceWrongTypes("1", "a"), InterfaceWrongTypes, 1))
            err = try
                Avro.Writer(writeone, IOBuffer(), person)
                nothing
            catch caught
                caught
            end
            @test err isa Avro.EncodeError && err.path == "\$.id"
        end
        dw = Avro.DatumWriter(person, InterfacePerson)
        @test dw(people[1]) == Avro.encode(person, people[1])
        err = try
            Avro.DatumWriter(person, InterfaceWrongTypes)(InterfaceWrongTypes("1", "a"))
            nothing
        catch caught
            caught
        end
        @test err isa Avro.EncodeError && err.path == "\$.id"
        @test collect(Avro.Rows(Avro.tobuffer([1, 2, 3]))) == [1, 2, 3]
        @test collect(Avro.Rows(Avro.tobuffer(["a", "b"]))) == ["a", "b"]
        @test collect(Avro.Rows(Avro.tobuffer((i for i in 1:3); schema=Avro.LongSchema()))) == 1:3
        @test collect(Avro.Rows(Avro.tobuffer([interface_red, interface_green]))) == ["interface_red", "interface_green"]
        for one in ("abc", 1, (a=1,), InterfacePerson(1, "a"), Dict("a" => 1))
            @test_throws ArgumentError Avro.tobuffer(one)
        end
    end

    @testset "Avro.write takes iterators of rows and narrows open column types" begin
        @test Avro.Table(Avro.tobuffer((a=i,) for i in 1:3)).a == 1:3
        @test Avro.Table(Avro.tobuffer(Iterators.map(i -> (a=i,), 1:3))).a == 1:3
        @test Avro.Table(Avro.tobuffer(((a=i,) for i in 1:3); schema=Avro.schema((a=1,)))).a == 1:3
        @test Avro.Table(Avro.tobuffer(Any[(a=1,), (a=2,)])).a == [1, 2]
        t = Avro.Table(Avro.tobuffer((a=Any[1, "x"], b=Any[1, 2], c=Any[missing, 1.5], d=Real[1, 2])))
        @test eltype(t.b) === Int64 && eltype(t.c) === Union{Missing,Float64} && eltype(t.d) === Int64
        @test t.a == [1, "x"] && isequal(t.c, [missing, 1.5])
        @test isequal(Avro.Table(Avro.tobuffer([(a=1,), (a="x",), (a=missing,)])).a, [1, "x", missing])
        for (source, pattern) in (((a=Any[],), r"column a of type Any"),
                                  ((b=[Dict(1 => 2)],), r"column b of type Dict\{Int64, ?Int64\}"))
            err = try
                Avro.tobuffer(source)
                nothing
            catch caught
                caught
            end
            @test err isa ArgumentError && occursin(pattern, err.msg)
        end
        nested = P("""{"type":"record","name":"O","fields":[{"name":"r","type":{"type":"record","name":"I","fields":[{"name":"x","type":"long"}]}}]}""")
        records = Avro.Table(Avro.tobuffer([(r=(x=1,),)]; schema=nested))
        err = try
            Avro.tobuffer((r=records.r,))
            nothing
        catch caught
            caught
        end
        @test err isa ArgumentError && occursin("column r", err.msg) && occursin("schema=", err.msg)
        @test Avro.Table(Avro.tobuffer((r=records.r,); schema=nested)).r == records.r
    end

    @testset "select= takes one name; rows; show" begin
        buf = take!(Avro.tobuffer([(a=1, b="x")]))
        @test Avro.Table(buf; select=:b).b == ["x"] && Avro.Table(buf; select="a").a == [1]
        @test collect(Tables.columnnames(Avro.Table(buf; select=:b))) == [:b]
        @test collect(Avro.Rows(buf; select=:a))[1] == Avro.Record(Avro.schema(Avro.Rows(buf; select=:a)), [1])
        @test_throws Avro.SchemaError Avro.Table(buf; select=:c)
        @test isequal(Tables.rowtable(Avro.Table(Avro.tobuffer([(a=1, b=missing), (a=2, b="x")]))),
                      NamedTuple{(:a, :b),Tuple{Int64,Union{Missing,String}}}[(a=1, b=missing), (a=2, b="x")])
        @test Tables.rowtable(Avro.Rows(buf)) == [(a=1, b="x")]
        @test collect(Tables.namedtupleiterator(Avro.Table(buf))) == [(a=1, b="x")]
        @test sprint(show, Avro.Rows(buf)) == "Avro.Rows(1 rows: a, b)"
        @test sprint(show, Avro.Rows(buf; T=NamedTuple{(:a, :b),Tuple{Int,String}})) == "Avro.Rows(1 rows: @NamedTuple{a::Int64, b::String})"
        @test sprint(show, Avro.Rows(take!(Avro.tobuffer([1])))) == "Avro.Rows(1 rows: long)"
        @test sprint(show, Avro.Rows(IOBuffer(buf))) == "Avro.Rows(1 rows: a, b)"
        s = Avro.schema(Avro.Table(buf))
        store = Avro.SchemaCache()
        @test sprint(show, Avro.DatumReader(s)) == "Avro.DatumReader(record Record)"
        @test sprint(show, Avro.DatumReader(Avro.LongSchema(), Int)) == "Avro.DatumReader(long, Int64)"
        @test sprint(show, Avro.DatumWriter(s)) == "Avro.DatumWriter(record Record)"
        @test sprint(show, Avro.SingleObjectWriter(s)) == "Avro.SingleObjectWriter(record Record)"
        @test sprint(show, Avro.SingleObjectReader(store)) == "Avro.SingleObjectReader(Avro.SchemaCache)"
        @test sprint(show, Avro.Encoder()) == "Avro.Encoder(0 bytes)"
        reader = Avro.Reader(buf)
        @test sprint(show, reader) == "Avro.Reader(record Record, codec=null)"
        close(reader)
        @test sprint(show, reader) == "Avro.Reader(record Record, codec=null, closed)"
        writer = Avro.Writer(IOBuffer(), s)
        @test sprint(show, writer) == "Avro.Writer(record Record, codec=null)"
        close(writer)
        @test sprint(show, writer) == "Avro.Writer(record Record, codec=null, closed)"
        open_stream = Base.BufferStream()
        Base.write(open_stream, buf)
        close(open_stream)
        @test sprint(show, Avro.Rows(open_stream)) == "Avro.Rows(a, b)"
    end

    @testset "select= keeps a repaired schema's flags" begin
        s = P("""{"type":"record","name":"R","fields":[{"name":"bad-name","type":"long"},{"name":"ok","type":"string"}]}""";
              allow_invalid_names=true)
        bytes = take!(Avro.tobuffer([(var"bad-name"=1, ok="x")]; schema=s, allow_invalid_names=true))
        t = Avro.Table(bytes; select=:ok, allow_invalid_names=true)
        @test t.ok == ["x"]
        @test_throws ArgumentError Avro.tobuffer(t)
        @test Avro.Table(Avro.tobuffer(t; allow_invalid_names=true); allow_invalid_names=true).ok == ["x"]
        d = P("""{"type":"record","name":"D","fields":[{"name":"a","type":"long","default":"no"},{"name":"b","type":"string"}]}""";
              allow_invalid_defaults=true)
        bytes = take!(Avro.tobuffer([(a=1, b="x")]; schema=d, allow_invalid_defaults=true))
        t = Avro.Table(bytes; select=:b, allow_invalid_defaults=true)
        @test t.b == ["x"]
    end

    @testset "encoders take limits" begin
        nested = P("""{"type":"array","items":{"type":"array","items":"long"}}""")
        value = [[1]]
        low = Avro.Limits(max_depth=1)
        @test_throws Avro.LimitError Avro.encode(nested, value; limits=low)
        @test_throws Avro.LimitError Avro.encode!(IOBuffer(), nested, value; limits=low)
        @test_throws Avro.LimitError Avro.DatumWriter(nested; limits=low)(value)
        @test_throws Avro.LimitError Avro.encodesingle(nested, value; limits=low)
        @test_throws Avro.LimitError Avro.tojson(nested, value; limits=low)
        @test_throws Avro.LimitError Avro.SingleObjectWriter(nested; limits=low)(value)
        list = P("""{"type":"record","name":"L","fields":[{"name":"n","type":["null","L"]}]}""")
        function chain(n)
            value = Dict{String,Any}("n" => missing)
            for _ in 1:n
                value = Dict{String,Any}("n" => value)
            end
            return value
        end
        depth = Avro.Limits().max_depth
        @test_throws Avro.LimitError Avro.encode(list, chain(depth + 5))
        @test !isempty(Avro.encode(list, chain(depth + 5); limits=Avro.Limits(max_depth=2 * depth + 20)))
    end

    @testset "append" begin
        rows(a) = [(id=Int64(i), name="n$i") for i in a]
        mktempdir() do dir
            for codec in (:null, :deflate, :snappy, :zstandard)
                path = joinpath(dir, "a-$codec.avro")
                @test Avro.write(path, rows(1:2); append=true, codec=codec, metadata=Dict("k" => b"v")) == path    # creates
                Avro.write(path, rows(3:3); append=true)
                Avro.write(path, rows(4:5); append=true, codec=codec)
                t = Avro.Table(path)
                @test t.id == 1:5 && Avro.codec(t) === codec && Avro.metadata(t)["k"] == b"v"
                @test isempty(Avro.inspect(path).issues) && Avro.inspect(path).datums == 5
                @test length(Avro.Reader(r -> collect(Avro.eachblock(r)), path)) == 3
            end
            path = joinpath(dir, "b.avro")
            Avro.write(path, rows(1:1))
            original = read(path)
            @test_throws ArgumentError Avro.write(path, rows(2:2); append=true, schema=person)
            @test_throws ArgumentError Avro.write(path, rows(2:2); append=true, codec=:deflate)
            @test_throws ArgumentError Avro.write(path, rows(2:2); append=true, metadata=Dict("k" => b"v"))
            @test_throws ArgumentError Avro.write(path, rows(2:2); append=true, sync=zeros(UInt8, 16))
            @test_throws Avro.EncodeError Avro.write(path, [(id="no", name="x")]; append=true)
            @test read(path) == original
            s = Avro.writerschema(Avro.Table(path))
            w = Avro.Writer(path, s; append=true)
            push!(w, rows(2:2)[1])
            flush(w)
            @test filesize(path) > length(original)
            close(w; abort=true)
            @test read(path) == original
            @test_throws Avro.EncodeError Avro.Writer(w -> push!(w, (id="no", name="x")), path, s; append=true)
            @test read(path) == original
            @test_throws ArgumentError Avro.Writer(IOBuffer(), s; append=true)
            Avro.Writer(path, s; append=true) do w
                push!(w, rows(2:2)[1])
            end
            @test Avro.Table(path).id == [1, 2]
            torn = joinpath(dir, "torn.avro")
            write(torn, read(path)[1:end - 1])
            @test_throws Avro.DataError Avro.write(torn, rows(3:3); append=true)
            notavro = joinpath(dir, "text.avro")
            write(notavro, "not avro at all, long enough to hold a tail")
            @test_throws Avro.AvroError Avro.write(notavro, rows(1:1); append=true)
            empty = joinpath(dir, "empty.avro")
            touch(empty)
            Avro.write(empty, rows(1:1); append=true)
            @test Avro.Table(empty).id == [1]
            retained = joinpath(dir, "retained.avro")
            Avro.write(retained, Avro.Table(path))
            Avro.write(retained, Avro.Table(path); append=true)
            @test Avro.Table(retained).id == [1, 2, 1, 2]
        end
    end

    @testset "prepared single-object codecs" begin
        store = Avro.SchemaCache()
        fp = Avro.register!(store, person)
        T = NamedTuple{(:id, :name),Tuple{Int64,String}}
        value = (id=Int64(5), name="five")
        w = Avro.SingleObjectWriter(person)
        msg = w(value)
        @test msg == Avro.encodesingle(person, value) == Avro.SingleObjectWriter(person, T)(value)
        @test msg[1:2] == [0xc3, 0x01] && reinterpret(UInt64, msg[3:10])[1] == htol(fp)
        @test_throws Avro.EncodeError w((id="no", name="x"))
        @test w(value) == msg                                  # a failure leaves no header behind
        r = Avro.SingleObjectReader(store)
        @test r(msg).id == 5 && r(IOBuffer(msg)).name == "five" && r(view(msg, 1:length(msg))).id == 5
        @test Avro.SingleObjectReader(store, T)(msg) === value
        @test Avro.decodesingle(msg, store; T=T) === value && Avro.decodesingle(msg, store).id == 5
        wider = P("""{"type":"record","name":"Person","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"},
                      {"name":"age","type":"int","default":7}]}""")
        @test Avro.SingleObjectReader(store; reader_schema=wider)(msg).age == 7
        @test_throws Avro.DataError r(vcat(msg, 0x00))
        @test_throws Avro.DataError r(msg[1:5])
        @test_throws Avro.DataError r(vcat(UInt8[0, 0], msg[3:end]))
        @test_throws Avro.UnknownSchemaError Avro.SingleObjectReader(Avro.SchemaCache())(msg)
        struct_store = CountingStore(store, Ref(0))
        counted = Avro.SingleObjectReader(struct_store)
        for _ in 1:100
            counted(msg)
        end
        @test struct_store.lookups[] == 1
        tasks = [Threads.@spawn(sum(_ -> r(msg).id, 1:2_000)) for _ in 1:4]
        @test all(t -> fetch(t) == 10_000, tasks)
        @test_throws Avro.DataError Avro.SingleObjectReader(LyingStore(person))(Avro.encodesingle(wider, (id=1, name="x", age=2)))
    end

    @testset "schema references" begin
        address = """{"type":"record","name":"Address","namespace":"org","fields":[{"name":"city","type":"string"}]}"""
        person2 = """{"type":"record","name":"Person","namespace":"org","fields":[{"name":"home","type":"Address"},{"name":"work","type":"org.Address"}]}"""
        s = P(person2; references=[address])
        inline = P("""{"type":"record","name":"Person","namespace":"org","fields":[{"name":"home","type":$address},{"name":"work","type":"Address"}]}""")
        @test s == inline && Avro.fingerprint(s) == Avro.fingerprint(inline) && Avro.json(s) == Avro.json(inline)
        @test Avro.decode(s, Avro.encode(s, (home=(city="a",), work=(city="b",)))).work.city == "b"
        @test P(person2; references=[P(address)]) == inline
        @test P(person2; references=[IOBuffer(address)]) == inline
        @test_throws Avro.SchemaError P(person2)
        @test_throws Avro.SchemaError P(person2; references=[address, address])
        unused = """{"type":"enum","name":"Unused","symbols":["A"]}"""
        @test !occursin("Unused", Avro.json(P(person2; references=[unused, address])))
        chained = """{"type":"record","name":"Team","namespace":"org","fields":[{"name":"lead","type":"Person"}]}"""
        team = P(chained; references=[address, person2])
        @test Avro.decode(team, Avro.encode(team, (lead=(home=(city="a",), work=(city="b",)),))).lead.home.city == "a"
        @test_throws Avro.SchemaError P(chained; references=[person2, address])
    end

    @testset "uncompressed blocks read in place" begin
        rows = [(s="value-$i", b=UInt8[i % UInt8], f=i / 2) for i in 1:10_000]
        bytes = take!(Avro.tobuffer(rows; block_bytes=4096))
        expected = deepcopy(Tables.columntable(Avro.Table(copy(bytes))))
        for ntasks in (1, 4)
            source = copy(bytes)
            t = Avro.Table(source; ntasks=ntasks)
            fill!(source, 0x00)                                # decoded values do not alias the source
            @test t.s == expected.s && t.b == expected.b && t.f == expected.f
        end
    end

    @testset "writes never lose a value silently" begin
        rows(a) = [(id=Int64(i), name="n$i") for i in a]
        # a source's extra columns
        @test_throws ArgumentError Avro.tobuffer([(id=1, name="a", extra=2)]; schema=person)
        @test_throws ArgumentError Avro.tobuffer((id=[1], name=["a"], extra=[2]); schema=person)
        @test_throws ArgumentError Avro.tobuffer(DataFrames.DataFrame(id=[1], name=["a"], extra=[2]); schema=person)
        @test_throws ArgumentError Avro.tobuffer(Tables.partitioner([[Dict(:a => 1)], [Dict(:a => 2, :b => 3)]]))
        mktempdir() do dir
            path = joinpath(dir, "extra.avro")
            Avro.write(path, rows(1:1))
            @test_throws ArgumentError Avro.write(path, [(id=2, name="b", extra=3)]; append=true)
            @test Avro.Table(path).id == [1]
        end
        @test_throws ArgumentError Avro.tobuffer(NamedTuple[])
        # integers a float type cannot hold
        f, d = P("\"float\""), P("\"double\"")
        @test_throws Avro.EncodeError Avro.encode(f, 16777217)
        @test Avro.decode(f, Avro.encode(f, 16777216)) === 1.6777216f7
        @test_throws Avro.EncodeError Avro.encode(d, 2^53 + 1)
        @test Avro.decode(d, Avro.encode(d, 2^53)) === 2.0^53
        @test_throws Avro.EncodeError Avro.encode(d, big(2)^2000)
        @test_throws Avro.EncodeError Avro.encode(d, BigFloat(1) + BigFloat(2)^-200)
        @test Avro.decode(d, Avro.encode(d, BigFloat(1.5))) === 1.5
        @test isnan(Avro.decode(d, Avro.encode(d, BigFloat(NaN))))
        @test Avro.encode(P("[\"float\",\"double\"]"), 16777217)[1] == 0x02       # the branch that holds it
        @test Avro.encode(P("[\"float\",\"double\"]"), 1)[1] == 0x00
        @test_throws Avro.EncodeError Avro.encode(P("[\"int\",\"float\"]"), 2^40 + 1)
        # rows of differing types keep their values
        mixed = Avro.Table(Avro.tobuffer([(a=2^53 + 1,), (a=0.5,), (a=true,)]))
        @test mixed.a[1] === 2^53 + 1 && mixed.a[2] === 0.5 && mixed.a[3] === true
        # a union of two decimal scales re-encodes each value under its own branch
        scales = P("""[{"type":"bytes","logicalType":"decimal","precision":5,"scale":2},
                       {"type":"fixed","name":"D4","size":8,"logicalType":"decimal","precision":10,"scale":4}]""")
        for value in (Avro.Decimal(Int128(125), 2), Avro.Decimal(Int128(125), 4))
            @test Avro.decode(scales, Avro.encode(scales, value)) == value
        end
    end

    @testset "sources Avro.write reads and refuses" begin
        s = P("""{"type":"record","name":"K","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["A"]}},
                 {"name":"f","type":{"type":"fixed","name":"F","size":1}},{"name":"o","type":["null","long"]}]}""")
        records = collect(Avro.Rows(Avro.tobuffer([(e="A", f=UInt8[1], o=1), (e="A", f=UInt8[2], o=missing)]; schema=s)))
        # records keep their schema
        rewritten = Avro.Table(Avro.tobuffer(records))
        @test Avro.schema(rewritten) == s && rewritten.f[2] == UInt8[2] && eltype(rewritten.e) === Avro.EnumValue
        for one in (records[1], UInt8[1, 2], Avro.Fixed(Avro.FixedSchema("B", 1), UInt8[1]), "a" => 1, [1 2; 3 4], (1, 2), Set([1]), 1, "abc")
            @test_throws ArgumentError Avro.tobuffer(one)
        end
        df = DataFrames.DataFrame(id=[1, 2], name=["a", "b"])
        @test_throws ArgumentError Avro.tobuffer(df[1, :])
        # a source of partitions that is not itself a table
        io = IOBuffer()
        Arrow.write(io, Tables.partitioner([(a=[1, 2],), (a=[3],)]))
        @test Avro.Table(Avro.tobuffer(Arrow.Stream(take!(io)))).a == [1, 2, 3]
        chunks = CSV.Chunks(IOBuffer("a,b\n1,x\n2,y\n"); ntasks=1)
        @test Avro.Table(Avro.tobuffer(chunks)).b == ["x", "y"]
        # a row whose properties are its columns
        @test Avro.decode(person, Avro.encode(person, df[2, :])).name == "b"
        counter = DataFrames.DataFrame(rownumber=[100, 200])
        rownumber = P("""{"type":"record","name":"N","fields":[{"name":"rownumber","type":"long"}]}""")
        @test Avro.decode(rownumber, Avro.encode(rownumber, counter[2, :])).rownumber == 200
        @test Avro.Table(Avro.tobuffer(eachrow(df))).id == [1, 2]
        # an empty file still has columns
        empty = take!(Avro.tobuffer(NamedTuple{(:a,),Tuple{Int}}[]))
        io = IOBuffer()
        Arrow.write(io, Avro.Rows(empty))
        @test collect(Tables.columnnames(Arrow.Table(take!(io)))) == [:a]
        @test collect(Tables.columnnames(Avro.Table(Avro.tobuffer(Avro.Rows(empty))))) == [:a]
    end

    @testset "metadata" begin
        table = [(a=1,)]
        strings = Avro.Table(Avro.tobuffer(table; metadata=Dict("k" => "v", "b" => b"\x00")))
        @test Avro.metadata(strings)["k"] == b"v" && Avro.metadata(strings)["b"] == b"\x00"
        @test Avro.Table(Avro.tobuffer(table; metadata=Dict())).a == [1]
        @test_throws ArgumentError Avro.tobuffer(table; metadata=Dict(1 => "v"))
        @test_throws ArgumentError Avro.tobuffer(table; metadata=Dict("k" => 1))
        @test_throws ArgumentError Avro.tobuffer(table; metadata=[1])
        # a rewrite keeps user metadata unless told otherwise
        source = take!(Avro.tobuffer(table; metadata=Dict("k" => "v")))
        for rewrite in (Avro.Table(source), Avro.Rows(source))
            @test Avro.metadata(Avro.Table(Avro.tobuffer(rewrite)))["k"] == b"v"
        end
        replaced = Avro.Table(Avro.tobuffer(Avro.Table(source); metadata=Dict("n" => "w")))
        @test !haskey(Avro.metadata(replaced), "k") && Avro.metadata(replaced)["n"] == b"w"
        mktempdir() do dir
            path = joinpath(dir, "m.avro")
            write(path, source)
            Avro.write(path, Avro.Table(source); append=true)   # an append keeps the file's own metadata
            @test Avro.Table(path).a == [1, 1] && Avro.metadata(Avro.Table(path))["k"] == b"v"
        end
    end

    @testset "append leaves other writers' data alone" begin
        rows(a) = [(id=Int64(i), name="n$i") for i in a]
        mktempdir() do dir
            path = joinpath(dir, "a.avro")
            Avro.write(path, rows(1:1))
            s = Avro.writerschema(Avro.Table(path; mmap=false))
            # a writer dropped after a caught failure, then another append, then the finalizer
            dropped = Avro.Writer(path, s; append=true)
            @test_throws Avro.EncodeError push!(dropped, (id="no", name="x"))
            Avro.write(path, rows(2:3); append=true)
            finalize(dropped)
            @test Avro.Table(path; mmap=false).id == [1, 2, 3]
            # an explicit abort after another writer appended
            aborted = Avro.Writer(path, s; append=true)
            Avro.write(path, rows(4:4); append=true)
            close(aborted; abort=true)
            @test Avro.Table(path; mmap=false).id == [1, 2, 3, 4]
            # a flushed block survives a writer that is never closed
            flushed = Avro.Writer(path, s; append=true)
            push!(flushed, rows(5:5)[1])
            flush(flushed)
            finalize(flushed)
            @test Avro.Table(path; mmap=false).id == [1, 2, 3, 4, 5]
            # a failed write removes what it added
            before = read(path)
            failed = Avro.Writer(path, s; append=true, block_bytes=1)
            push!(failed, rows(6:6)[1])
            @test filesize(path) >= length(before)
            @test_throws Avro.EncodeError push!(failed, (id="no", name="x"))
            close(failed)
            @test read(path) == before
            # a link to the file stays a link
            link = joinpath(dir, "link.avro")
            symlink(path, link)
            Avro.write(link, rows(7:7); append=true)
            @test islink(link) && Avro.Table(path; mmap=false).id[end] == 7
        end
    end

    @testset "small interface edges" begin
        @test Avro.truncate(Time(1, 2, 3, 456), Second) == Time(1, 2, 3)
        @test Avro.truncate(Time(1, 2, 3, 456, 789), Millisecond) == Time(1, 2, 3, 456)
        @test_throws ArgumentError Avro.schema(Union{})
    end

    @testset "closing a mapped source releases its file" begin
        mktempdir() do dir
            path = joinpath(dir, "m.avro")
            Avro.write(path, [(a=i, s="v$i") for i in 1:100])
            t = Avro.Table(path)
            reader = Avro.Reader(path)
            close(reader)
            @test t.a == 1:100                                  # a table never needs the map again
            rows = Avro.Rows(path)
            first(rows)
            close(rows)
            @test_throws ArgumentError first(rows)
            Avro.write(path, Avro.Table(path))                  # replacing the file it read from, then appending
            Avro.write(path, Avro.Table(path); append=true)
            @test Avro.Table(path).a == [1:100; 1:100]
        end
    end

    @testset "DataFrames and CSV" begin
        s = P("""{"type":"record","name":"R","fields":[{"name":"id","type":"long"},
                  {"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]}},
                  {"name":"f","type":{"type":"fixed","name":"F","size":2}},
                  {"name":"u","type":["long","string"]},{"name":"o","type":["null","string"]}]}""")
        rows = [(id=1, e="A", f=UInt8[1, 2], u=1, o=missing), (id=2, e="B", f=UInt8[3, 4], u="x", o="y")]
        t = Avro.Table(Avro.tobuffer(rows; schema=s, metadata=Dict("k" => b"v")))
        df = DataFrames.DataFrame(t)
        @test df.id == [1, 2] && df.e == ["A", "B"] && df.f == [UInt8[1, 2], UInt8[3, 4]] && df.u == [1, "x"]
        @test DataAPI.metadata(df, "k") == "v"
        @test Avro.Table(Avro.tobuffer(df; schema=s)).e == ["A", "B"]
        derived = Avro.Table(Avro.tobuffer(df))
        @test derived.e == ["A", "B"] && derived.u == [1, "x"] && eltype(derived.e) === String && eltype(derived.f) === Vector{UInt8}
        io = IOBuffer()
        CSV.write(io, (id=t.id, e=t.e))
        @test String(take!(io)) == "id,e\n1,A\n2,B\n"
        c = Avro.Table(Avro.tobuffer(CSV.File(IOBuffer("a,b\n1,x\n2,\n"))))
        @test c.a == [1, 2] && isequal(c.b, ["x", missing])
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

    @testset "TimeZones" begin
        denver = ZonedDateTime(2024, 1, 2, 3, tz"America/Denver")
        utc = TimeZones.astimezone(denver, tz"UTC")
        buf = take!(Avro.tobuffer([(at=denver,)]))
        t = Avro.Table(buf)
        @test Avro.schema(t).fields[1].schema.logical isa Avro.TimestampMillis
        @test t.at[1] == Avro.timestamp(Millisecond, DateTime(2024, 1, 2, 10))
        got = collect(Avro.Rows(buf; T=NamedTuple{(:at,),Tuple{ZonedDateTime}}))[1].at
        @test got == denver && got == utc && TimeZones.timezone(got) == tz"UTC"
        optional = take!(Avro.tobuffer([(at=denver,), (at=missing,)]))
        typed = collect(Avro.Rows(optional; T=NamedTuple{(:at,),Tuple{Union{Missing,ZonedDateTime}}}))
        @test typed[1].at == denver && ismissing(typed[2].at)
        for (logical, ticks) in (("timestamp-millis", 1704189600000), ("timestamp-micros", 1704189600000000), ("timestamp-nanos", 1704189600000000000))
            s = P("""{"type":"long","logicalType":"$logical"}""")
            @test Avro.decode(Avro.LongSchema(), Avro.encode(s, denver)) == ticks
            @test Avro.decode(s, Avro.encode(s, denver)) == denver
        end
        @test_throws Avro.EncodeError Avro.encode(P("""{"type":"long","logicalType":"local-timestamp-millis"}"""), denver)
        @test Avro.encode(P("""["null",{"type":"long","logicalType":"timestamp-millis"}]"""), denver)[1] == 0x02
        @test Avro.encode(P("""["string",{"type":"long","logicalType":"timestamp-millis"}]"""), denver)[1] == 0x02
    end
end
