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
        wrapped = vcat(UInt8[0xff], wbin, UInt8[0xff])
        @test Avro.decodesingle(view(wrapped, 2:length(wrapped) - 1), store) == w1
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
        base = Avro.schemacachebasebytes()
        @test_throws ArgumentError Avro.SchemaCache(max_bytes=base - 1)
        emptycache = Avro.SchemaCache(max_entries=0, max_bytes=base)
        @test emptycache.bytes == emptycache.ledger.reserved == base
        @test Base.summarysize(emptycache; exclude=Avro.Schema) == base
        small = Avro.SchemaCache(max_bytes=base)
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
            seen::Base.RefValue{Union{Nothing,Avro.Budget}}
        end

        function Avro.lookup(s::DictStore, fp::UInt64; limits=Avro.Limits())
            return haskey(s.d, fp) ? s.d[fp] : throw(Avro.UnknownSchemaError(fp))
        end

        function Avro.lookup(s::DictStore, fp::UInt64, budget::Avro.Budget)
            s.seen[] = budget
            return Avro.lookup(s, fp; limits=budget.limits)
        end

        function Avro.register!(s::DictStore, schema::Avro.Schema;
                                limits=Avro.Limits())
            fp = Avro.fingerprint(schema)
            s.d[fp] = schema
            return fp
        end

        ds = DictStore(Dict{UInt64,Avro.Schema}(), Ref{Union{Nothing,Avro.Budget}}(nothing))
        Avro.register!(ds, ws)
        @test Avro.decodesingle(wbin, ds) == w1
        @test ds.seen[] isa Avro.Budget
        # a store returning a schema under the wrong fingerprint is rejected
        liar = DictStore(Dict(Avro.fingerprint(ws) => P("\"int\"")), Ref{Union{Nothing,Avro.Budget}}(nothing))
        @test_throws Avro.DataError Avro.decodesingle(wbin, liar)
        e = try Avro.decodesingle(wbin, DictStore(Dict{UInt64,Avro.Schema}(), Ref{Union{Nothing,Avro.Budget}}(nothing))); nothing catch x; x end
        @test e isa Avro.UnknownSchemaError && e.fingerprint == Avro.fingerprint(ws)
        @test occursin("fingerprint", sprint(showerror, e))

        struct LegacyStore <: Avro.SchemaStore
            d::Dict{UInt64,Avro.Schema}
        end

        function Avro.lookup(s::LegacyStore, fp::UInt64; limits=Avro.Limits())
            return haskey(s.d, fp) ? s.d[fp] : throw(Avro.UnknownSchemaError(fp))
        end

        legacy = LegacyStore(Dict(Avro.fingerprint(ws) => ws))
        @test_throws ArgumentError Avro.decodesingle(wbin, legacy)              # no nested operation budget fallback
    end

    @testset "one operation budget; transactional exact-capacity cache (plan §4.4, amendment round 1)" begin
        s1 = P("{\"type\":\"record\",\"name\":\"C1\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        s2 = P("{\"type\":\"record\",\"name\":\"C2\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        c = Avro.SchemaCache(max_entries=1)
        fp1 = Avro.register!(c, s1)
        @test_throws Avro.LimitError Avro.register!(c, s2)                               # the failed insert left the table untouched
        @test length(c) == 1 && Avro.lookup(c, fp1) === s1
        cb = Avro.SchemaCache(max_bytes=Avro.schemacachebasebytes())
        @test_throws Avro.LimitError Avro.register!(cb, s1)
        @test length(cb) == 0
        c2 = Avro.SchemaCache()
        for s in (s1, s2)
            Avro.register!(c2, s)
        end
        @test length(c2.fingerprints) == length(c2.schemas) == 2                          # exact-capacity replacement
        nullschema = P("\"null\"")
        longschema = P("\"long\"")
        probe = Avro.SchemaCache(max_bytes=1 << 20)
        Avro.register!(probe, nullschema)
        chargebudget = Avro.Budget(Avro.Limits(); available=1 << 30)
        longcharge = Avro.schemaretainedbytes(longschema, chargebudget)
        Avro.close!(chargebudget)
        exactlimit = probe.bytes + longcharge + Avro.cacheindexbytes(2)
        peakbounded = Avro.SchemaCache(max_bytes=exactlimit - 1)
        nullfp = Avro.register!(peakbounded, nullschema)
        @test_throws Avro.LimitError Avro.register!(peakbounded, longschema)        # old and new index vectors overlap
        @test length(peakbounded) == 1 && Avro.lookup(peakbounded, nullfp) isa Avro.NullSchema
        exactpeak = Avro.SchemaCache(max_bytes=exactlimit)
        Avro.register!(exactpeak, nullschema)
        Avro.register!(exactpeak, longschema)
        @test length(exactpeak) == 2 && exactpeak.bytes == exactpeak.ledger.reserved
        @test exactpeak.ledger.peak == exactpeak.max_bytes
        largeprops = Avro.IntSchema(props=(x=repeat("x", 1_000_000),))
        @test_throws Avro.LimitError Avro.register!(Avro.SchemaCache(max_bytes=Avro.schemacachebasebytes()), largeprops)
        msg = Avro.encodesingle(s1, (a=Int64(7),))
        @test Avro.decodesingle(msg, c2).a === Int64(7)
        big = Avro.encodesingle(s1, (a=typemax(Int64),))
        @test Avro.decodesingle(big, c2).a === typemax(Int64)

        stringschema = P("{\"type\":\"record\",\"name\":\"CS\",\"fields\":[{\"name\":\"s\",\"type\":\"string\"}]}")
        stringcache = Avro.SchemaCache()
        Avro.register!(stringcache, stringschema)
        stringmessage = Avro.encodesingle(stringschema, (s="abc",))
        viewbytes = vcat(UInt8[0xff], stringmessage, UInt8[0xff])
        viewmessage = view(viewbytes, 2:length(viewbytes) - 1)
        vectorbudget = Avro.Budget(Avro.Limits(); available=1 << 30)
        vectorvalue, _, _ = Avro.decodesingleoperation(stringmessage, stringcache, vectorbudget;
                                                       limits=vectorbudget.limits)
        viewbudget = Avro.Budget(Avro.Limits(); available=1 << 30)
        viewvalue, _, _ = Avro.decodesingleoperation(viewmessage, stringcache, viewbudget;
                                                     limits=viewbudget.limits)
        @test viewvalue == vectorvalue == Avro.Record(stringschema, ["abc"])
        @test Avro.storagebytes(viewvalue) == Avro.heldbytes(viewvalue)
        @test viewbudget.reserved == vectorbudget.reserved
        @test viewbudget.peak == vectorbudget.peak
        @test viewbudget.pending == vectorbudget.pending == 0
        Avro.close!(vectorbudget)
        Avro.close!(viewbudget)

        compareschema = P("\"string\"")
        comparecache = Avro.SchemaCache()
        Avro.register!(comparecache, compareschema)
        comparelimits = Avro.Limits(work_allowance=0,
                                    max_compare_bytes_per_byte=1,
                                    max_values_per_byte=1)
        comparevalue = repeat("x", 20)
        comparemessage = Avro.encodesingle(compareschema, comparevalue)
        @test length(comparemessage) - 10 == 21
        @test Avro.decodesingle(comparemessage, comparecache;
                                limits=comparelimits) == comparevalue
        @test Avro.decodesingle(IOBuffer(comparemessage), comparecache;
                                limits=comparelimits) == comparevalue

        shortvalue = repeat("x", 14)
        shortmessage = Avro.encodesingle(compareschema, shortvalue)
        @test length(shortmessage) - 10 == 15
        compareerror = try
            Avro.decodesingle(shortmessage, comparecache; limits=comparelimits)
            nothing
        catch error
            error
        end
        @test compareerror isa Avro.LimitError
        @test compareerror.limit === :max_compare_bytes_per_byte
        @test compareerror.observed == 16
        @test compareerror.value == 15

        workschema = P("{\"type\":\"record\",\"name\":\"WorkSymmetry\",\"fields\":[{\"name\":\"n\",\"type\":\"null\"},{\"name\":\"s\",\"type\":\"string\"}]}")
        workvalue = Avro.Record(workschema, Any[missing, "xxx"])
        strictwork = Avro.Limits(work_allowance=0,
                                 max_values_per_byte=1,
                                 max_compare_bytes_per_byte=typemax(Int))
        encodeerror = try
            Avro.encodesingle(workschema, workvalue; limits=strictwork)
            nothing
        catch error
            error
        end
        @test encodeerror isa Avro.LimitError
        @test encodeerror.limit === :max_values_per_byte
        @test encodeerror.observed == 6
        @test encodeerror.value == 4

        boundarywork = Avro.Limits(work_allowance=2,
                                   max_values_per_byte=1,
                                   max_compare_bytes_per_byte=typemax(Int))
        workmessage = Avro.encodesingle(workschema, workvalue;
                                        limits=boundarywork)
        @test length(workmessage) - 10 == 4
        workcache = Avro.SchemaCache()
        Avro.register!(workcache, workschema)
        workroundtrip = Avro.decodesingle(workmessage, workcache;
                                          limits=boundarywork)
        @test workroundtrip.n === missing
        @test workroundtrip.s == "xxx"
    end
end
