@testset "Storage formulas (plan §4.4 (b))" begin
    @testset "layout probes equal the recorded constants" begin
        @test Avro.STORAGE[] == Avro.RECORDED_STORAGE
        @test Avro.measurestorage() == Avro.RECORDED_STORAGE
        @test sizeof(Avro.Record) == 16 && sizeof(Avro.EnumValue) == 16 && sizeof(Avro.Fixed) == 16 && sizeof(Avro.UnionValue) == 16
        @test Avro.slotbytes(Int32) == 4 && Avro.slotbytes(Union{Missing,Int64}) == 9 && Avro.slotbytes(Union{Missing,String}) == 8 && Avro.slotbytes(Any) == 8
        @test Avro.vectorbytes(Union{Missing,Int32}, 1000) == 40 + 5000 && Avro.bytesbytes(3) == 43 && Avro.stringbytes(3) == 19
        @test Avro.boxbytes(Int64) == 24 && Avro.boxbytes(Avro.Decimal) == 48 && Avro.recordbytes(3) == 80 && Avro.fixedbytes(3) == 59
        @test Avro.boxcharge(Int32) == 20 && Avro.boxcharge(Union{Missing,Int32}) == 20 && Avro.boxcharge(String) == 0 && Avro.boxcharge(Union{Missing,String}) == 0
        @test Avro.mapbytes(Int64, 2) == 24 + 3 * 40 + 2 * (4 + 8 + 8)
    end
    @testset "storagebytes ≥ summarysize for every member of E; decoders reserve at least the formula" begin
        rng = Random.Xoshiro(11)
        ss(x) = Base.summarysize(x; exclude=Avro.Schema)
        function decoded(s, bytes)
            return Avro.withbudget(Avro.Limits()) do budget
                Avro.addinput!(budget, length(bytes))
                d = Avro.Decoder(bytes, budget)
                w = Avro.decode(Avro.readplan(s), d)
                (w, budget.peak)
            end
        end
        checked = 0
        for T in Avro.valuetypes()
            src = schemafor(T)
            src === nothing && continue
            s = Avro.parseschema(src)
            for pick in (0, 1)
                v = samplevalue(s, rng, pick)
                w, peak = decoded(s, Avro.encode(s, v))
                @test Avro.heldbytes(w) >= ss(w)
                isbits(w) || @test peak >= Avro.storagebytes(w)
                checked += 1
            end
        end
        @test checked > 400
        # larger shapes: wide records, long strings, nested collections, duplicate-heavy and prefix-heavy maps
        P = Avro.parseschema
        rec = P("{\"type\":\"record\",\"name\":\"W\",\"fields\":[" * join(["{\"name\":\"f$i\",\"type\":$(rand(rng, ("\"int\"", "\"string\"", "\"double\"", "[\"null\",\"long\"]", "{\"type\":\"array\",\"items\":\"bytes\"}")))}" for i in 1:64], ",") * "]}")
        rv = samplevalue(rec, rng)
        w, peak = decoded(rec, Avro.encode(rec, rv))
        @test Avro.storagebytes(w) >= ss(w) && peak >= Avro.storagebytes(w)
        arr = P("{\"type\":\"array\",\"items\":{\"type\":\"map\",\"values\":{\"type\":\"array\",\"items\":\"string\"}}}")
        av = [Avro.Map([("k$(i)$(j)", ["s"^j for _ in 1:j]) for j in 1:5]) for i in 1:50]
        w, peak = decoded(arr, Avro.encode(arr, av))
        @test Avro.storagebytes(w) >= ss(w) && peak >= Avro.storagebytes(w)
        ms = P("{\"type\":\"map\",\"values\":\"long\"}")
        rawmap(keys) = vcat(Avro.encode(P("\"long\""), length(keys)), [vcat(Avro.encode(P("\"string\""), k), Avro.encode(P("\"long\""), i)) for (i, k) in enumerate(keys)]..., UInt8[0x00])   # duplicates stay on the wire
        for keys in (["k" for _ in 1:1000], ["x"^100 * string(i) for i in 1000:-1:1], [string(i) for i in 1:1001], String[])
            w, peak = decoded(ms, rawmap(keys))
            @test Avro.storagebytes(w) >= ss(w) && peak >= Avro.storagebytes(w)
        end
        ds = P("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":400,\"scale\":2}")
        for k in (1, 60, 63, 64, 100, 300, 500, 1000), sign in (1, -1)
            w, peak = decoded(ds, Avro.encode(ds, Avro.WideDecimal(sign * big(2)^k, 2)))
            @test Avro.storagebytes(w) >= ss(w) && peak >= Avro.storagebytes(w)
        end
        # identity-bearing values with shared and distinct schema identities
        es = P("{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"a\",\"b\"]}")
        ev = [Avro.EnumValue(es, 1), Avro.EnumValue(P("{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"a\",\"b\"]}"), 2)]
        @test Avro.storagebytes(ev) == 40 + 2 * 16 + 2 * 16 && Avro.storagebytes(ev) >= ss(ev)  # inline slots plus the values charged at production
        @test Avro.storagebytes(Any[ev[1]]) == 40 + 8 + 16 && Avro.heldbytes(ev[1]) == 16
        @test_throws ArgumentError Avro.storagebytes(Dict("a" => 1))
    end
end
