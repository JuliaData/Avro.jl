@testset "Single-object encoding and schema stores" begin
    P = Avro.parseschema
    ws = P(read(joinpath(FIXTURES, "generated", "singleobject", "weather.avsc"), String))
    wbin = read(joinpath(FIXTURES, "generated", "singleobject", "weather1.bin"))
    w1 = Avro.fromjson(ws, read(joinpath(FIXTURES, "generated", "singleobject", "weather1.json"), String))
    store = Avro.SchemaCache()

    @testset "encodesingle / decodesingle against Java output" begin
        @test Avro.encodesingle(ws, w1) == wbin
        @test wbin[1:2] == UInt8[0xc3, 0x01]
        fp = Avro.fingerprint(ws)
        @test reinterpret(UInt64, wbin[3:10])[1] == fp          # little-endian fingerprint
        @test_throws Avro.UnknownSchemaError Avro.decodesingle(wbin, store)
        @test Avro.register!(store, ws) == fp && length(store) == 1
        @test Avro.register!(store, ws) == fp && length(store) == 1                       # idempotent
        @test Avro.register!(store, P(Avro.json(ws))) == fp && length(store) == 1         # structurally equal copy
        @test Avro.decodesingle(wbin, store) == w1
        @test Avro.decodesingle(IOBuffer(wbin), store) == w1
        @test Avro.decodesingle(wbin, store; T=NamedTuple{(:station, :time, :temp),Tuple{String,Int,Int}}) == (station="011990-99999", time=-619524000000, temp=0)
        nullschema = Avro.NullSchema()
        Avro.register!(store, nullschema)
        nullmessage = Avro.encodesingle(nullschema, nothing)
        @test Avro.decodesingle(nullmessage, store) === missing
        @test Avro.decodesingle(nullmessage, store; T=Nothing) === nothing
        @test_throws ArgumentError Avro.decodesingle(nullmessage, store; T=1)
        @test Avro.lookup(store, fp) === ws
        @test_throws Avro.UnknownSchemaError Avro.lookup(store, fp + 1)
        # Apache messageV1 fixture
        ms = P(read(joinpath(FIXTURES, "apache", "messageV1", "test_schema.avsc"), String))
        mbin = read(joinpath(FIXTURES, "apache", "messageV1", "test_message.bin"))
        Avro.register!(store, ms)
        m = Avro.decodesingle(mbin, store)
        @test m.id == 42 && m.name == "Bill" && m.tags == ["dog_lover", "cat_hater"]
        @test Avro.encodesingle(ms, m) == mbin
        # malformed messages
        @test_throws Avro.DataError Avro.decodesingle(wbin[1:9], store)
        @test_throws Avro.DataError Avro.decodesingle(vcat(UInt8[0xc3, 0x02], wbin[3:end]), store)
        @test_throws Avro.DataError Avro.decodesingle(vcat(wbin, UInt8[0x00]), store)            # trailing byte
        @test_throws Avro.DataError Avro.decodesingle(wbin[1:end - 1], store)                    # truncated payload
        @test_throws Avro.LimitError Avro.decodesingle(wbin, store; limits=Avro.Limits(max_datum_bytes=10, max_bytes=10))
        @test Avro.decodesingle(wbin, store; validate=:fast) == w1
        @test_throws ArgumentError Avro.decodesingle(wbin, store; validate=:loose)
    end

    @testset "SchemaCache ambiguity and bounds" begin
        c = Avro.SchemaCache(max_entries=2)
        rec = P("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        err = P("{\"type\":\"error\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        @test Avro.canonical(rec) == Avro.canonical(err)
        fp = Avro.register!(c, rec)
        @test_throws Avro.AmbiguousSchemaError Avro.register!(c, err)                          # error vs record share a PCF
        withdefault = P("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":\"long\",\"default\":1}]}")
        @test Avro.fingerprint(withdefault) == fp
        @test_throws Avro.AmbiguousSchemaError Avro.register!(c, withdefault)                  # parsing-equivalent, different default
        withlogical = P("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}]}")
        @test_throws Avro.AmbiguousSchemaError Avro.register!(c, withlogical)
        withprops = P("{\"type\":\"record\",\"name\":\"R\",\"doc\":\"d\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        @test_throws Avro.AmbiguousSchemaError Avro.register!(c, withprops)
        e = try Avro.register!(c, err); nothing catch x; x end
        @test e isa Avro.AmbiguousSchemaError && e.fingerprint == fp && e.existing === rec && e.offered === err
        @test Avro.register!(c, P("\"int\"")) == Avro.fingerprint(P("\"int\""))
        @test_throws Avro.LimitError Avro.register!(c, P("\"string\""))                         # max_entries
        @test length(c) == 2
        small = Avro.SchemaCache(max_bytes=100)
        @test_throws Avro.LimitError Avro.register!(small, ws)
        @test_throws ArgumentError Avro.SchemaCache(max_entries=-1)
        # registration is safe across tasks
        big = Avro.SchemaCache()
        schemas = [P("{\"type\":\"record\",\"name\":\"T$i\",\"fields\":[]}") for i in 1:32]
        tasks = [Threads.@spawn Avro.register!(big, s) for s in schemas, _ in 1:4]
        foreach(errormonitor, tasks)
        foreach(fetch, tasks)
        @test length(big) == 32
        @test all(Avro.lookup(big, Avro.fingerprint(s)) === s for s in schemas)
    end

    @testset "custom stores" begin
        struct DictStore <: Avro.SchemaStore
            d::Dict{UInt64,Avro.Schema}
        end
        Avro.lookup(s::DictStore, fp::UInt64; limits=Avro.Limits()) = haskey(s.d, fp) ? s.d[fp] : throw(Avro.UnknownSchemaError(fp))
        Avro.register!(s::DictStore, schema::Avro.Schema; limits=Avro.Limits()) = (fp = Avro.fingerprint(schema); s.d[fp] = schema; fp)
        ds = DictStore(Dict{UInt64,Avro.Schema}())
        Avro.register!(ds, ws)
        @test Avro.decodesingle(wbin, ds) == w1
        # a store returning a schema under the wrong fingerprint is rejected
        liar = DictStore(Dict(Avro.fingerprint(ws) => P("\"int\"")))
        @test_throws Avro.DataError Avro.decodesingle(wbin, liar)
        e = try Avro.decodesingle(wbin, DictStore(Dict{UInt64,Avro.Schema}())); nothing catch x; x end
        @test e isa Avro.UnknownSchemaError && e.fingerprint == Avro.fingerprint(ws)
        @test occursin("fingerprint", sprint(showerror, e))
    end

    @testset "one operation budget; transactional exact-capacity cache (plan §4.4, amendment round 1)" begin
        s1 = P("{\"type\":\"record\",\"name\":\"C1\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        s2 = P("{\"type\":\"record\",\"name\":\"C2\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        c = Avro.SchemaCache(max_entries=1)
        fp1 = Avro.register!(c, s1)
        @test_throws Avro.LimitError Avro.register!(c, s2)                               # the failed insert left the table untouched
        @test length(c) == 1 && Avro.lookup(c, fp1) === s1
        cb = Avro.SchemaCache(max_bytes=8)
        @test_throws Avro.LimitError Avro.register!(cb, s1)
        @test length(cb) == 0
        c2 = Avro.SchemaCache()
        for s in (s1, s2)
            Avro.register!(c2, s)
        end
        @test length(c2.fingerprints) == length(c2.schemas) == 2                          # exact-capacity replacement
        msg = Avro.encodesingle(s1, (a=Int64(7),))
        @test Avro.decodesingle(msg, c2).a === Int64(7)
        big = Avro.encodesingle(s1, (a=typemax(Int64),))
        @test Avro.decodesingle(big, c2).a === typemax(Int64)
    end
end
