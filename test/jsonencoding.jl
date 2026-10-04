@testset "JSON encoding" begin
    P = Avro.parseschema
    function tj(s, x; kw...)
        return Avro.tojson(P(s), x; kw...)
    end

    function fj(s, t; kw...)
        return Avro.fromjson(P(s), t; kw...)
    end

    @testset "rule table: output and accepted input" begin
        @test tj("\"null\"", missing) == "null" && fj("\"null\"", "null") === missing
        @test_throws Avro.DataError fj("\"null\"", "0")
        @test tj("\"boolean\"", true) == "true" && fj("\"boolean\"", "false") === false
        @test_throws Avro.DataError fj("\"boolean\"", "1")
        @test tj("\"int\"", Int32(-5)) == "-5" && fj("\"int\"", "-5") === Int32(-5) && fj("\"long\"", "9223372036854775807") === typemax(Int64)
        @test_throws Avro.DataError fj("\"int\"", "2147483648")
        @test_throws Avro.DataError fj("\"int\"", "1.0")
        @test_throws Avro.DataError fj("\"long\"", "\"1\"")
        @test_throws Avro.DataError fj("\"long\"", "9223372036854775808")
        @test tj("\"float\"", 0.1f0) == "0.1" && tj("\"float\"", 1f10) == "1.0e10" && tj("\"double\"", 1e-5) == "1.0e-5" && tj("\"double\"", -0.0) == "-0.0"
        @test fj("\"float\"", "0.1") === 0.1f0 && fj("\"double\"", "1") === 1.0 && fj("\"double\"", "1e400") === Inf && fj("\"float\"", "-1e-60") === -0.0f0
        for (x, t) in ((NaN, "\"NaN\""), (Inf, "\"Infinity\""), (-Inf, "\"-Infinity\""))
            @test tj("\"double\"", x) == t && tj("\"float\"", Float32(x)) == t
            v = fj("\"double\"", t)
            @test isequal(v, x) && isequal(fj("\"float\"", t), Float32(x))
        end
        @test_throws Avro.DataError fj("\"double\"", "\"nan\"")
        @test_throws Avro.DataError fj("\"double\"", "NaN")                      # bare tokens are not JSON
        @test isnan(fj("\"double\"", "NaN"; strict=false)) && fj("\"double\"", "Infinity"; strict=false) === Inf && fj("\"float\"", "-Infinity"; strict=false) === -Inf32
        @test_throws Avro.DataError fj("\"long\"", "NaN"; strict=false)          # only float/double accept them
        @test_throws Avro.DataError fj("\"double\"", "Nan"; strict=false)
        @test_throws Avro.DataError fj("\"double\"", "[NaN"; strict=false)
        # bytes / fixed: code points U+0000–U+00FF
        @test tj("\"bytes\"", UInt8[0x00, 0x22, 0x5c, 0x7f, 0x80, 0xff]) == "\"\\u0000\\\"\\\\\x7f\u0080\u00ff\""
        @test fj("\"bytes\"", "\"\\u0000\\\"\\\\\x7f\u0080\u00ff\"") == UInt8[0x00, 0x22, 0x5c, 0x7f, 0x80, 0xff]
        @test_throws Avro.DataError fj("\"bytes\"", "\"\u0100\"")
        @test_throws Avro.DataError fj("\"bytes\"", "[1]")
        f = "{\"type\":\"fixed\",\"name\":\"F\",\"size\":2}"
        @test tj(f, UInt8[0xc3, 0xa9]) == "\"\u00c3\u00a9\"" && fj(f, "\"\u00c3\u00a9\"") == Avro.Fixed(P(f), UInt8[0xc3, 0xa9])
        @test_throws Avro.DataError fj(f, "\"abc\"")
        fixedat = Avro.Limits(max_bytes=2, max_datum_bytes=16)
        fixedover = Avro.Limits(max_bytes=1, max_datum_bytes=16)
        @test tj(f, UInt8[0xc3, 0xa9]; limits=fixedat) == "\"Ã©\""
        @test fj(f, "\"Ã©\""; limits=fixedat) ==
              Avro.Fixed(P(f), UInt8[0xc3, 0xa9])
        @test_throws Avro.LimitError tj(f, UInt8[0xc3, 0xa9]; limits=fixedover)
        @test_throws Avro.LimitError fj(f, "\"Ã©\""; limits=fixedover)
        @test tj("\"string\"", "héllo\n") == "\"héllo\\n\"" && fj("\"string\"", "\"h\\u00e9llo\\n\"") == "héllo\n"
        @test_throws Avro.DataError fj("\"string\"", "\"\\ud800\"")             # lone surrogate
        @test_throws Avro.DataError fj("\"string\"", "1")
        e = "{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"A\",\"B\"]}"
        @test tj(e, :B) == "\"B\"" && fj(e, "\"B\"") == Avro.EnumValue(P(e), 2)
        @test_throws Avro.DataError fj(e, "\"C\"")
        @test_throws Avro.DataError fj(e, "1")
        arr = "{\"type\":\"array\",\"items\":\"int\"}"
        @test tj(arr, [1, 2]) == "[1,2]" && tj(arr, Int[]) == "[]" && fj(arr, "[1, 2]") == Int32[1, 2] && fj(arr, "[]") == Int32[]
        @test_throws Avro.DataError fj(arr, "{}")
        mp = "{\"type\":\"map\",\"values\":\"long\"}"
        @test tj(mp, Dict("b" => 1, "a" => 2)) in ("{\"a\":2,\"b\":1}", "{\"b\":1,\"a\":2}")
        @test fj(mp, "{\"b\":1,\"a\":2}") == Avro.Map{Int64}([("a", 2), ("b", 1)]) && tj(mp, Avro.Map([("x", 1)])) == "{\"x\":1}"
        @test_throws Avro.DataError fj(mp, "{\"a\":1,\"a\":2}")                   # duplicate keys
        @test_throws Avro.DataError fj(mp, "[]")
        r = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":[\"null\",\"string\"]}]}"
        @test tj(r, (a=1, b="x")) == "{\"a\":1,\"b\":{\"string\":\"x\"}}" && tj(r, (a=1, b=missing)) == "{\"a\":1,\"b\":null}"
        @test fj(r, "{\"b\":{\"string\":\"x\"},\"a\":1}") == Avro.Record(P(r), [1, "x"])
        @test fj(r, "{\"a\":1,\"b\":null}") isa Avro.Record && fj(r, "{\"a\":1,\"b\":null}").b === missing
        @test fj(r, "{\"a\":1,\"b\":null,\"zzz\":[1,2]}").a === Int32(1)         # unknown members ignored
        @test_throws Avro.DataError fj(r, "{\"a\":1,\"b\":null,\"zzz\":1}"; unknown=:error)
        @test_throws Avro.DataError fj(r, "{\"a\":1}")                           # missing field (no defaults in fromjson)
        @test_throws Avro.DataError fj(r, "{\"a\":1,\"b\":{\"string\":\"x\",\"int\":1}}")
        @test_throws Avro.DataError fj(r, "{\"a\":1,\"b\":{\"long\":1}}")
        @test_throws Avro.DataError fj(r, "{\"a\":1,\"b\":\"x\"}")
        @test_throws ArgumentError fj(r, "{}"; unknown=:maybe)
        @test fj(r, "{\"a\":1,\"b\":{\"null\":null}}").b === missing
        u = "[\"int\",{\"type\":\"record\",\"name\":\"ns.Rec\",\"fields\":[]},{\"type\":\"fixed\",\"name\":\"Fx\",\"size\":1},\"null\"]"
        @test tj(u, Int32(1)) == "{\"int\":1}" && tj(u, Avro.UnionValue(4, missing)) == "null" && tj(u, Avro.Record(P(u).branches[2], [])) == "{\"ns.Rec\":{}}"
        @test fj(u, "{\"ns.Rec\":{}}") == Avro.UnionValue(2, Avro.Record(P(u).branches[2], [])) && fj(u, "{\"Fx\":\"a\"}") == Avro.UnionValue(3, Avro.Fixed(P(u).branches[3], UInt8[0x61]))
        @test isequal(fj(u, "null"), Avro.UnionValue(4, missing))
        @test_throws Avro.DataError fj("[\"int\",\"string\"]", "null")
        amb = "[{\"type\":\"record\",\"name\":\"map\",\"fields\":[]},{\"type\":\"map\",\"values\":\"int\"}]"
        @test_throws Avro.EncodeError tj(amb, Avro.Record(P(amb).branches[1], []))
        @test_throws Avro.DataError fj(amb, "{\"map\":{}}")
        @test_throws Avro.EncodeError tj(amb, Avro.UnionValue(2, Avro.Map([("k", 1)])))
    end

    @testset "logical types" begin
        @test tj("{\"type\":\"int\",\"logicalType\":\"date\"}", Date(1970, 1, 2)) == "1" && fj("{\"type\":\"int\",\"logicalType\":\"date\"}", "-1") == Date(1969, 12, 31)
        @test tj("{\"type\":\"int\",\"logicalType\":\"time-millis\"}", Time(1)) == "3600000" && fj("{\"type\":\"int\",\"logicalType\":\"time-millis\"}", "3600000") == Time(1)
        @test_throws Avro.DataError fj("{\"type\":\"int\",\"logicalType\":\"time-millis\"}", "86400000")
        @test tj("{\"type\":\"long\",\"logicalType\":\"time-micros\"}", Time(0, 0, 0, 0, 1)) == "1" && fj("{\"type\":\"long\",\"logicalType\":\"time-micros\"}", "1") == Time(0, 0, 0, 0, 1)
        @test_throws Avro.DataError fj("{\"type\":\"long\",\"logicalType\":\"time-micros\"}", "-1")
        ts = "{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}"
        @test tj(ts, DateTime(1970, 1, 1, 0, 0, 1)) == "1000000" && fj(ts, "1000000") == Avro.Timestamp{Microsecond}(1_000_000)
        @test fj("{\"type\":\"long\",\"logicalType\":\"local-timestamp-nanos\"}", "-1") == Avro.LocalTimestamp{Nanosecond}(-1)
        db = "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":5,\"scale\":2}"
        @test tj(db, Avro.Decimal(-129, 2)) == "\"\u00ff\u007f\"" && fj(db, "\"\u00ff\u007f\"") == Avro.Decimal(-129, 2)
        @test_throws Avro.DataError fj(db, "\"\"")
        @test_throws Avro.DataError fj(db, "\"\u0001\u0086\u00a0\"")              # 100000 exceeds precision 5
        df = "{\"type\":\"fixed\",\"name\":\"D\",\"size\":4,\"logicalType\":\"decimal\",\"precision\":9}"
        @test tj(df, Avro.Decimal(-1, 0)) == "\"\u00ff\u00ff\u00ff\u00ff\"" && tj(df, Avro.Decimal(1, 0)) == "\"\\u0000\\u0000\\u0000\\u0001\""
        @test fj(df, "\"\u00ff\u00ff\u00ff\u00ff\"") == Avro.Decimal(-1, 0)
        wide = "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":50}"
        w = Avro.WideDecimal(big(10)^45, 0)
        @test fj(wide, tj(wide, w)) == w
        us = "{\"type\":\"string\",\"logicalType\":\"uuid\"}"
        id = UUID("123e4567-e89b-12d3-a456-426614174000")
        @test tj(us, id) == "\"123e4567-e89b-12d3-a456-426614174000\"" && fj(us, "\"123E4567-E89B-12D3-A456-426614174000\"") == id
        @test_throws Avro.DataError fj(us, "\"nope\"")

        hostilebytes = 1_000_000
        hostile = repeat("x", hostilebytes)
        quotedhostile = string('"', hostile, '"')
        hostilelimits = Avro.Limits(max_bytes=hostilebytes,
                                    max_datum_bytes=hostilebytes + 10)
        uuidfailure = () -> try
            Avro.fromjson(P(us), quotedhostile; limits=hostilelimits)
        catch err
            err
        end
        uuidfailure()
        uuiderror = uuidfailure()
        @test uuiderror isa Avro.DataError
        @test sizeof(uuiderror.msg) < 256
        @test occursin("999904 bytes", uuiderror.msg)
        @test @allocated(uuidfailure()) < 3 * hostilebytes
        hostilecases = (
            (P("{\"type\":\"enum\",\"name\":\"HostileEnum\",\"symbols\":[\"A\"]}"),
             quotedhostile, (;)),
            (P("{\"type\":\"record\",\"name\":\"HostileRecord\",\"fields\":[]}"),
             string("{", quotedhostile, ":null}"), (; unknown=:error)),
            (P("[\"null\",\"int\"]"), string("{", quotedhostile, ":0}"), (;)),
        )
        for (hostileschema, hostilejson, options) in hostilecases
            err = try
                Avro.fromjson(hostileschema, hostilejson;
                              limits=hostilelimits, options...)
            catch caught
                caught
            end
            @test err isa Avro.DataError
            @test sizeof(err.msg) < 256
            @test occursin("999904 bytes", err.msg)
        end

        uf = "{\"type\":\"fixed\",\"name\":\"U\",\"size\":16,\"logicalType\":\"uuid\"}"
        @test fj(uf, tj(uf, id)) == id
        dur = "{\"type\":\"fixed\",\"name\":\"Du\",\"size\":12,\"logicalType\":\"duration\"}"
        x = Avro.Duration(UInt32(1), UInt32(2), UInt32(3))
        @test tj(dur, x) == "\"\\u0001\\u0000\\u0000\\u0000\\u0002\\u0000\\u0000\\u0000\\u0003\\u0000\\u0000\\u0000\"" && fj(dur, tj(dur, x)) == x

        budget = Avro.Budget(Avro.Limits(); available=1 << 30)
        sink = Avro.BoundedWriter(budget, 1024; limit=:max_datum_bytes)
        for printer in (Avro.printuuidstring, Avro.printuuidbytestring)
            printer(sink, id)
            sink.len = 0
            @test @allocated(printer(sink, id)) == 0
            sink.len = 0
        end
        Avro.printdurationbytestring(sink, x)
        sink.len = 0
        @test @allocated(Avro.printdurationbytestring(sink, x)) == 0
        Avro.close!(budget)
    end

    @testset "JSON conversion ownership" begin
        function parsedjson(text::String, budget::Avro.Budget)
            bytes = Vector{UInt8}(codeunits(text))
            Avro.addinput!(budget, length(bytes))
            errfn = (msg, pos) -> throw(Avro.DataError(msg, pos))
            limitfn = (limit, observed, value) -> throw(Avro.LimitError(limit, observed, value, limit, :decode))
            return Avro.parsejson(bytes; maxbytes=budget.limits.max_datum_bytes,
                                  maxdepth=budget.limits.max_json_depth, errfn=errfn,
                                  budget=budget, limitfn=limitfn,
                                  bytelimit=:max_datum_bytes, depthlimit=:max_json_depth)
        end

        stringbudget = Avro.Budget(Avro.Limits(); available=1 << 30)
        parsedstring = parsedjson("\"x\"", stringbudget)
        parsedcharge = stringbudget.reserved
        convertedstring = Avro.jsontovalue(P("\"string\""), parsedstring,
                                            Avro.JSONContext(stringbudget, true, false, false), 1)
        @test convertedstring === parsedstring
        @test stringbudget.reserved == parsedcharge
        Avro.close!(stringbudget)

        cases = (
            ("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":5}", "\"\\u0001\"", 1),
            ("{\"type\":\"fixed\",\"name\":\"OwnedUuid\",\"size\":16,\"logicalType\":\"uuid\"}",
             "\"" * "\\u0000"^16 * "\"", 16),
            ("{\"type\":\"fixed\",\"name\":\"OwnedDuration\",\"size\":12,\"logicalType\":\"duration\"}",
             "\"" * "\\u0000"^12 * "\"", 12),
        )
        for (itemschema, itemjson, payloadbytes) in cases
            schema = P("{\"type\":\"array\",\"items\":" * itemschema * "}")
            text = "[" * join(fill(itemjson, 8), ",") * "]"
            budget = Avro.Budget(Avro.Limits(); available=1 << 30)
            parsed = parsedjson(text, budget)
            before = budget.reserved
            value = Avro.jsontovalue(schema, parsed, Avro.JSONContext(budget, true, false, false), 1)
            @test length(value) == 8
            @test budget.reserved - before == Avro.vectorbytes(Avro.elementtype(schema.items), 8)
            Avro.close!(budget)

            tight = Avro.Budget(Avro.Limits(); available=1 << 30)
            tightparsed = parsedjson(text, tight)
            finalbytes = Avro.vectorbytes(Avro.elementtype(schema.items), 8)
            needed = finalbytes + Avro.bytesbytes(payloadbytes)
            filler = tight.ceiling - tight.reserved - needed
            Avro.reserve!(tight, filler)
            tightvalue = Avro.jsontovalue(schema, tightparsed,
                                           Avro.JSONContext(tight, true, false, false), 1)
            @test length(tightvalue) == 8
            @test tight.reserved == tight.ceiling - Avro.bytesbytes(payloadbytes)
            Avro.close!(tight)
        end

        fixedschema = P("{\"type\":\"fixed\",\"name\":\"OwnedFixed\",\"size\":1}")
        fixedbudget = Avro.Budget(Avro.Limits(); available=1 << 30)
        fixedparsed = parsedjson("\"x\"", fixedbudget)
        fixedbefore = fixedbudget.reserved
        fixedvalue = Avro.jsontovalue(fixedschema, fixedparsed,
                                      Avro.JSONContext(fixedbudget, true, false, false), 1)
        @test fixedvalue.bytes == UInt8['x']
        @test fixedbudget.reserved - fixedbefore == Avro.fixedbytes(1)
        Avro.close!(fixedbudget)
    end

    @testset "pretty printing, limits, typed targets, fixtures" begin
        r = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},{\"name\":\"m\",\"type\":{\"type\":\"map\",\"values\":\"int\"}},{\"name\":\"u\",\"type\":[\"null\",\"int\"]}]}"
        pretty = tj(r, (a=[1], m=Dict("k" => 2), u=3); pretty=true)
        @test pretty == "{\n  \"a\": [\n    1\n  ],\n  \"m\": {\n    \"k\": 2\n  },\n  \"u\": {\n    \"int\": 3\n  }\n}"
        @test tj(r, (a=Int[], m=Dict{String,Int}(), u=missing); pretty=true) == "{\n  \"a\": [],\n  \"m\": {},\n  \"u\": null\n}"
        @test fj(r, pretty) == fj(r, tj(r, (a=[1], m=Dict("k" => 2), u=3)))
        # output bound, value bound, depth bound
        @test_throws Avro.LimitError tj("\"string\"", "x"^100; limits=Avro.Limits(max_datum_bytes=50, max_bytes=200))
        jsonwork = P("{\"type\":\"record\",\"name\":\"JSONWork\",\"fields\":[{\"name\":\"averylongname\",\"type\":\"null\"}]}")
        jsonvalue = (averylongname=missing,)
        jsonworklimits = Avro.Limits(work_allowance=0,
                                     max_values_per_byte=1,
                                     max_compare_bytes_per_byte=typemax(Int))
        @test Avro.tojson(jsonwork, jsonvalue;
                          limits=jsonworklimits) == "{\"averylongname\":null}"
        jsoncomparelimits = Avro.Limits(work_allowance=2,
                                        max_values_per_byte=1,
                                        max_compare_bytes_per_byte=2)
        @test Avro.tojson(jsonwork, jsonvalue;
                          limits=jsoncomparelimits) == "{\"averylongname\":null}"
        @test Avro.tojson(Avro.DoubleSchema(), 0.0;
                          limits=Avro.Limits(max_bytes=3,
                                             max_datum_bytes=3,
                                             work_allowance=100)) == "0.0"
        @test_throws Avro.LimitError tj("{\"type\":\"array\",\"items\":\"null\"}", fill(missing, 100); limits=Avro.Limits(max_total_values=50))
        @test_throws Avro.LimitError fj("{\"type\":\"array\",\"items\":\"null\"}", "[" * "null,"^99 * "null]"; limits=Avro.Limits(max_total_values=50))
        @test fj("[\"int\",\"string\"]", "{\"int\":0}"; limits=Avro.Limits(max_total_values=4)) == Avro.UnionValue(1, Int32(0))
        @test_throws Avro.LimitError fj("[\"int\",\"string\"]", "{\"int\":0}"; limits=Avro.Limits(max_total_values=3))
        @test fj("[\"null\",\"int\"]", "null"; limits=Avro.Limits(max_total_values=3)) === missing
        @test_throws Avro.LimitError fj("[\"null\",\"int\"]", "null"; limits=Avro.Limits(max_total_values=2))
        @test_throws Avro.LimitError fj("\"string\"", "\"" * "x"^100 * "\""; limits=Avro.Limits(max_datum_bytes=50, max_bytes=200))
        unionzero = Avro.Limits(max_depth=0)
        @test fj("[\"int\",\"string\"]", "{\"int\":1}";
                 limits=unionzero) == Avro.UnionValue(1, Int32(1))
        @test tj("[\"int\",\"string\"]", Avro.UnionValue(1, Int32(1));
                 limits=unionzero) == "{\"int\":1}"
        unionrecord = "[{\"type\":\"record\",\"name\":\"UnionRecord\",\"fields\":[]},\"null\"]"
        unionrecordlimits = Avro.Limits(max_depth=1)
        @test fj(unionrecord, "{\"UnionRecord\":{}}";
                 limits=unionrecordlimits) ==
              Avro.Record(P(unionrecord).branches[1], Any[])
        deep = P("{\"type\":\"record\",\"name\":\"D\",\"fields\":[{\"name\":\"n\",\"type\":[\"null\",\"D\"]}]}")
        function nest(k)
            v = Avro.Record(deep, [missing])
            for _ in 1:k
                v = Avro.Record(deep, [v])
            end
            return v
        end
        lim = Avro.Limits(max_depth=2000, max_json_depth=1024)
        text = Avro.tojson(deep, nest(510); limits=lim)            # 511 records + 511 union wrappers = 1022 levels
        @test isequal(Avro.fromjson(deep, text; limits=lim), nest(510))
        @test isequal(Avro.fromjson(deep, "{\"n\":null}";
                                    limits=Avro.Limits(max_depth=1)), nest(0))
        @test_throws Avro.LimitError Avro.fromjson(
            deep, "{\"n\":{\"D\":{\"n\":null}}}";
            limits=Avro.Limits(max_depth=1))
        @test_throws Avro.LimitError Avro.tojson(deep, nest(512); limits=lim)
        @test_throws Avro.LimitError Avro.fromjson(deep, "{\"n\":{\"D\":"^600 * "null" * "}}"^600; limits=lim)
        @test_throws Avro.DataError fj("\"int\"", "1 2")
        @test_throws Avro.DataError fj("\"int\"", "")
        @test fj("\"int\"", IOBuffer("7")) === Int32(7) && fj("\"int\"", Vector{UInt8}("7")) === Int32(7)
        # typed targets go through the semantic route with admission
        rr = P("{\"type\":\"record\",\"name\":\"T\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"s\",\"type\":\"string\"}]}")
        @test Avro.fromjson(rr, "{\"a\":1,\"s\":\"q\"}", NamedTuple{(:a, :s),Tuple{Int,Symbol}}) == (a=1, s=:q)
        @test_throws Avro.LimitError Avro.fromjson(rr, "{\"a\":1,\"s\":\"q\"}", NamedTuple{(:a, :s),Tuple{Int,Symbol}}; names=Avro.SymbolAdmission(max_names=0))
        @test Avro.fromjson(rr, "{\"a\":1,\"s\":\"q\"}", NamedTuple{(:a, :s),Tuple{Int,Symbol}}; names=:trusted) == (a=1, s=:q)
        @test Avro.fromjson(rr, "{\"a\":1,\"s\":\"q\"}", Avro.Record).a == 1 && Avro.fromjson(rr, "{\"a\":1,\"s\":\"q\"}", Any).s == "q"
        @test_throws Avro.ConversionError Avro.fromjson(rr, "{\"a\":1,\"s\":\"q\"}", Int)
        # Apache fixtures: weather records printed like Java's tojson
        ws = P(read(joinpath(FIXTURES, "generated", "singleobject", "weather.avsc"), String))
        j1 = read(joinpath(FIXTURES, "generated", "singleobject", "weather1.json"), String)
        v1 = Avro.fromjson(ws, j1)
        @test v1.station == "011990-99999" && v1.time == -619524000000 && v1.temp == 0
        @test Avro.fromjson(ws, Avro.tojson(ws, v1)) == v1
        @test Avro.tojson(ws, v1) == "{\"station\":\"011990-99999\",\"time\":-619524000000,\"temp\":0}"
        # round trip over the generated corpus: Java's tojson lines (`*.jsonl`) next to their schemas
        n = 0
        for dir in ("roots", "data", "evolution"), file in readdir(joinpath(FIXTURES, "generated", dir))
            endswith(file, ".jsonl") || continue
            avsc = joinpath(FIXTURES, "generated", dir, file[1:end - 6] * ".avsc")
            isfile(avsc) || continue
            s = P(read(avsc, String))
            for line in eachline(joinpath(FIXTURES, "generated", dir, file))
                isempty(strip(line)) && continue
                v = Avro.fromjson(s, line)
                @test isequal(Avro.fromjson(s, Avro.tojson(s, v)), v)
                @test Avro.encode(s, Avro.fromjson(s, Avro.tojson(s, v))) == Avro.encode(s, v)
                n += 1
            end
        end
        @test n > 0
    end

    @testset "defaults as values (bare unions, recursive record rule)" begin
        s = P("""{"type":"record","name":"R","fields":[
            {"name":"u","type":["null","int"],"default":null},
            {"name":"v","type":["int","null"],"default":3},
            {"name":"w","type":["string",{"type":"record","name":"In","fields":[{"name":"x","type":"int","default":7},{"name":"y","type":["null","In"],"default":null}]}],"default":{"x":1}},
            {"name":"f","type":"float","default":"NaN"}]}""")
        b = Avro.Budget(Avro.Limits(); available=1 << 40)
        Avro.addinput!(b, 1 << 20)
        d = Dict(f.name => Avro.jsonvalue(f.schema, f.default.json, b) for f in s.fields)
        @test d["u"] === missing && d["v"] === Int32(3)
        @test isequal(d["w"], Avro.UnionValue(2, Avro.Record(s.fields[3].schema.branches[2], [1, missing])))
        @test isnan(d["f"])
        inner = s.fields[3].schema.branches[2]
        @test isequal(Avro.jsonvalue(inner, Avro.parsejson("{}"; maxbytes=100, maxdepth=10, errfn=(m, p) -> error(m)), b), Avro.Record(inner, [7, missing]))
        @test_throws Avro.DataError Avro.jsonvalue(inner, Avro.parsejson("{\"x\":\"no\"}"; maxbytes=100, maxdepth=10, errfn=(m, p) -> error(m)), b)
    end
end
