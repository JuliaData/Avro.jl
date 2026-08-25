@testset "Schema resolution" begin
    P = Avro.parseschema
    rt(w, r, x; kw...) = Avro.decode(w, Avro.encode(w, x); reader_schema=r, kw...)
    @testset "resolve, policies, ResolvedSchema" begin
        w = P("\"int\"")
        rs = Avro.resolve(w, P("\"long\""))
        @test rs isa Avro.ResolvedSchema && rs.union_resolution === :spec && rs.plan isa Avro.PromotePlan
        promote_budget = Avro.Budget(Avro.Limits(); available=1 << 40)
        @test Avro.resolve(w, P("\"long\""); budget=promote_budget).plan isa Avro.PromotePlan
        @test promote_budget.pending == 0
        @test promote_budget.reserved == 64
        Avro.close!(promote_budget)

        array_budget = Avro.Budget(Avro.Limits(); available=1 << 40)
        array_writer = P("{\"type\":\"array\",\"items\":\"int\"}")
        array_reader = P("{\"type\":\"array\",\"items\":\"long\"}")
        @test Avro.resolve(array_writer, array_reader; budget=array_budget).plan isa Avro.ArrayPlan
        @test array_budget.pending == 0
        @test array_budget.reserved == 128
        Avro.close!(array_budget)
        @test_throws ArgumentError Avro.resolve(w, w; union_resolution=:odd)
        @test Avro.resolvingplan(w, w) isa Avro.IntPlan                        # writer == reader: the plain reader plan
    end
    @testset "promotions and failures" begin
        for (wk, rk, x, expect) in (("\"int\"", "\"long\"", Int32(5), Int64(5)), ("\"int\"", "\"float\"", Int32(5), 5.0f0),
                                    ("\"int\"", "\"double\"", Int32(5), 5.0), ("\"long\"", "\"float\"", Int64(3), 3.0f0),
                                    ("\"long\"", "\"double\"", Int64(3), 3.0), ("\"float\"", "\"double\"", 1.5f0, 1.5),
                                    ("\"string\"", "\"bytes\"", "héllo", Vector{UInt8}("héllo")), ("\"bytes\"", "\"string\"", Vector{UInt8}("héllo"), "héllo"))
            v = rt(P(wk), P(rk), x)
            @test v === expect || v == expect
            @test typeof(v) === typeof(expect)
        end
        for (wk, rk) in (("\"long\"", "\"int\""), ("\"double\"", "\"float\""), ("\"double\"", "\"long\""), ("\"string\"", "\"int\""), ("\"boolean\"", "\"int\""), ("\"null\"", "\"boolean\""))
            @test_throws Avro.ResolutionError Avro.resolve(P(wk), P(rk))
        end
        @test_throws Avro.DataError rt(P("\"bytes\""), P("\"string\""), UInt8[0xff])   # bytes→string validates UTF-8
    end
    @testset "logical pairings: the reader's interpretation wins over the raw value" begin
        @test rt(P("{\"type\":\"int\",\"logicalType\":\"date\"}"), P("\"int\""), Date(1970, 1, 3)) === Int32(2)
        @test rt(P("\"long\""), P("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}"), Int64(1500)) === Avro.Timestamp{Millisecond}(1500)
        @test rt(P("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}"), P("{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}"), Avro.Timestamp{Millisecond}(7)) ===
              Avro.Timestamp{Microsecond}(7)                                   # no unit conversion (documented hazard)
        @test rt(P("{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}"), P("{\"type\":\"long\",\"logicalType\":\"local-timestamp-micros\"}"), Avro.Timestamp{Microsecond}(9)) ===
              Avro.LocalTimestamp{Microsecond}(9)                              # global → local reinterprets
        @test rt(P("{\"type\":\"int\",\"logicalType\":\"date\"}"), P("{\"type\":\"int\",\"logicalType\":\"time-millis\"}"), Date(1970, 1, 2)) === Time(0, 0, 0, 1)
        @test rt(P("\"int\""), P("{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}"), Int32(4)) === Avro.Timestamp{Microsecond}(4)
        db = P("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":9,\"scale\":2}")
        @test rt(db, P("\"bytes\""), Avro.Decimal(150, 2)) == UInt8[0x00, 0x96]
        @test rt(P("\"bytes\""), db, UInt8[0x00, 0x96]) == Avro.Decimal(150, 2)
        @test rt(P("\"string\""), db, String(UInt8[0x01, 0x2c])) == Avro.Decimal(300, 2)   # promotion + interpretation (an ASCII payload: writer strings are valid UTF-8)
        @test_throws Avro.ResolutionError Avro.resolve(db, P("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":9,\"scale\":3}"))
        @test_throws Avro.ResolutionError Avro.resolve(db, P("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}"))
        us = P("{\"type\":\"string\",\"logicalType\":\"uuid\"}")
        @test_throws Avro.ResolutionError Avro.resolve(us, P("{\"type\":\"fixed\",\"name\":\"U\",\"size\":16,\"logicalType\":\"uuid\"}"))
        @test rt(P("\"string\""), us, "123e4567-e89b-12d3-a456-426614174000") == UUID("123e4567-e89b-12d3-a456-426614174000")
        @test_throws Avro.DataError rt(P("\"string\""), Avro.parseschema("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":9,\"scale\":2}"), "")
    end
    @testset "records: fields, aliases, defaults, order" begin
        w = P("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"string\"}]}")
        r = P("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"b\",\"type\":\"string\"},{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"c\",\"type\":{\"type\":\"array\",\"items\":\"int\"},\"default\":[1,2]}]}")
        v = rt(w, r, (a=7, b="x"))
        @test keys(v) == ["b", "a", "c"] && v.a === Int64(7) && v.b == "x" && v.c == Int32[1, 2]
        v2 = rt(w, r, (a=8, b="y"))
        push!(getfield(v, :values)[3], Int32(9))
        @test v2.c == Int32[1, 2]                                              # defaults are fresh per record
        rnodefault = P("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"z\",\"type\":\"int\"}]}")
        @test_throws Avro.ResolutionError Avro.resolve(w, rnodefault)
        # writer-only fields are skipped in both validation modes
        @test rt(w, P("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"b\",\"type\":\"string\"}]}"), (a=1, b="q"); validate=:fast).b == "q"
        # a reader field alias matches the writer field (an alias colliding with another reader field name
        # is already a SchemaError at parse time, so the consumed-name corner cannot arise)
        @test_throws Avro.SchemaError P("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"renamed\",\"type\":\"int\",\"aliases\":[\"a\"]},{\"name\":\"a\",\"type\":\"int\",\"default\":42}]}")
        ralias = P("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"renamed\",\"type\":\"int\",\"aliases\":[\"a\"]},{\"name\":\"b\",\"type\":\"string\"}]}")
        va = rt(w, ralias, (a=7, b="x"))
        @test va.renamed === Int32(7) && va.b == "x"
        ambiguous = P("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"c\",\"type\":\"int\",\"aliases\":[\"a\",\"b\"]}]}")
        @test_throws Avro.ResolutionError Avro.resolve(w, ambiguous)
        @test_throws Avro.SchemaError P("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"x\",\"type\":\"int\",\"aliases\":[\"a\"]},{\"name\":\"y\",\"type\":\"int\",\"aliases\":[\"a\"]}]}")   # a duplicate alias is a parse error
        nameandalias = P("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"b\",\"type\":\"string\",\"aliases\":[\"a\"]}]}")
        @test_throws Avro.ResolutionError Avro.resolve(w, nameandalias)         # its name and its alias match two writer fields
        # record type aliases and the unqualified-name match
        old = P("{\"type\":\"record\",\"name\":\"ns.Old\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}")
        new = P("{\"type\":\"record\",\"name\":\"ns.New\",\"aliases\":[\"Old\"],\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}")
        @test rt(old, new, (a=1,)).a === Int32(1)
        other = P("{\"type\":\"record\",\"name\":\"other.Old\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}")
        @test rt(old, other, (a=1,)).a === Int32(1)                            # same unqualified name
        @test_throws Avro.ResolutionError Avro.resolve(old, P("{\"type\":\"record\",\"name\":\"ns.Different\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}"))
        # recursive pair with reordered fields
        wl = P("{\"type\":\"record\",\"name\":\"LongList\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"},{\"name\":\"next\",\"type\":[\"null\",\"LongList\"],\"default\":null}]}")
        rl = P("{\"type\":\"record\",\"name\":\"LongList\",\"fields\":[{\"name\":\"next\",\"type\":[\"null\",\"LongList\"],\"default\":null},{\"name\":\"value\",\"type\":\"long\"}]}")
        chain = (value=1, next=(value=2, next=(value=3, next=missing)))
        out = rt(wl, rl, chain)
        @test out.value === Int64(1) && out.next.value === Int64(2) && out.next.next.value === Int64(3) && out.next.next.next === missing
    end
    @testset "enums" begin
        we = P("{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"A\",\"B\",\"C\"]}")
        re = P("{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"C\",\"A\"]}")
        v = rt(we, re, :A)
        @test v isa Avro.EnumValue && String(v) == "A" && v.index == 2 && v.schema === re
        @test String(rt(we, re, :C)) == "C"
        @test_throws Avro.ResolutionError rt(we, re, :B)                       # no reader default: raised only when selected
        red = P("{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"C\",\"A\"],\"default\":\"C\"}")
        @test String(rt(we, red, :B)) == "C"
        @test_throws Avro.ResolutionError Avro.resolve(we, P("{\"type\":\"enum\",\"name\":\"Other\",\"symbols\":[\"A\"]}"))
    end
    @testset "fixed" begin
        wf = P("{\"type\":\"fixed\",\"name\":\"F\",\"size\":2}")
        @test rt(wf, P("{\"type\":\"fixed\",\"name\":\"F2\",\"aliases\":[\"F\"],\"size\":2}"), UInt8[1, 2]).bytes == UInt8[1, 2]
        @test_throws Avro.ResolutionError Avro.resolve(wf, P("{\"type\":\"fixed\",\"name\":\"F\",\"size\":3}"))
        @test_throws Avro.ResolutionError Avro.resolve(wf, P("{\"type\":\"fixed\",\"name\":\"G\",\"size\":2}"))
    end
    @testset "unions: policies, directions, unresolvable branches" begin
        wi = P("\"int\"")
        @test rt(wi, P("[\"long\",\"int\"]"), Int32(5)) == Avro.UnionValue(1, Int64(5))                       # :spec: first match incl. promotion
        @test rt(wi, P("[\"long\",\"int\"]"), Int32(5); union_resolution=:java) == Avro.UnionValue(2, Int32(5))
        @test rt(wi, P("[\"double\",\"int\"]"), Int32(5)) == Avro.UnionValue(1, 5.0)
        @test rt(wi, P("[\"double\",\"int\"]"), Int32(5); union_resolution=:java) == Avro.UnionValue(2, Int32(5))
        @test rt(wi, P("[\"null\",\"long\"]"), Int32(5)) === Int64(5)                                          # nullable readers stay bare
        @test_throws Avro.ResolutionError Avro.resolve(wi, P("[\"string\",\"boolean\"]"))
        wu = P("[\"int\",\"string\"]")
        @test rt(wu, P("\"long\""), Avro.UnionValue(1, Int32(9))) === Int64(9)
        @test_throws Avro.ResolutionError rt(wu, P("\"long\""), Avro.UnionValue(2, "x"))                       # raised only when the datum selects it
        wn = P("[\"null\",\"int\"]")
        @test rt(wn, P("\"int\""), Int32(3)) === Int32(3)
        @test_throws Avro.ResolutionError rt(wn, P("\"int\""), missing)
        @test rt(wu, P("[\"string\",\"long\"]"), Avro.UnionValue(1, Int32(9))) == Avro.UnionValue(2, Int64(9))
        @test rt(wu, P("[\"string\",\"long\"]"), Avro.UnionValue(2, "x")) == Avro.UnionValue(1, "x")
        @test rt(P("[\"null\",\"string\"]"), P("[\"string\",\"null\"]"), missing) === missing
        @test rt(P("[\"null\",\"string\"]"), P("[\"string\",\"null\"]"), "q") == "q"
        @test_throws Avro.ResolutionError rt(P("[\"int\",\"boolean\"]"), P("\"long\""), Avro.UnionValue(2, true))
        @test rt(P("[\"int\",\"boolean\"]"), P("\"long\""), Avro.UnionValue(1, Int32(2))) === Int64(2)
    end
    @testset "work limit, repair, equality shortcut" begin
        wide(pfx, n) = P("[" * join(["{\"type\":\"record\",\"name\":\"$(pfx)$i\",\"fields\":[]}" for i in 1:n], ",") * "]")
        w, r = wide("W", 100), wide("R", 100)
        @test_throws Avro.LimitError Avro.resolve(w, r; limits=Avro.Limits(max_resolution_work=2000))
        wb = P("{\"type\":\"record\",\"name\":\"B\",\"fields\":[{\"name\":\"bad-name\",\"type\":\"int\"}]}"; allow_invalid_names=true)
        rb = P("{\"type\":\"record\",\"name\":\"B\",\"fields\":[{\"name\":\"good\",\"type\":\"int\",\"aliases\":[\"bad-name\"]}]}"; allow_invalid_names=true)
        bytes = Avro.encode(wb, (var"bad-name"=Int32(3),))
        @test Avro.decode(wb, bytes; reader_schema=rb).good === Int32(3)       # alias-based repair matches by exact bytes
        rid = P("{\"type\":\"record\",\"name\":\"B\",\"fields\":[{\"name\":\"only\",\"type\":\"int\",\"default\":\"nope\"}]}"; allow_invalid_defaults=true)
        @test_throws Avro.ResolutionError Avro.resolve(P("{\"type\":\"record\",\"name\":\"B\",\"fields\":[]}"), rid)
        ev = P(read(joinpath(FIXTURES, "generated", "schemas", "everything.avsc"), String))
        line = first(eachline(joinpath(FIXTURES, "generated", "data", "everything.jsonl")))
        v = Avro.fromjson(ev, line)
        bytes = Avro.encode(ev, v)
        @test isequal(Avro.decode(ev, bytes; reader_schema=ev), Avro.decode(ev, bytes))   # reader == writer equals the plain decode
    end
    @testset "Java oracle: the everything evolution fixtures" begin
        ev = P(read(joinpath(FIXTURES, "generated", "schemas", "everything.avsc"), String))
        lines = readlines(joinpath(FIXTURES, "generated", "data", "everything.jsonl"))
        for reader in ("everything_readerA", "everything_readerB")
            rs = P(read(joinpath(FIXTURES, "generated", "evolution", "$reader.avsc"), String))
            expected = readlines(joinpath(FIXTURES, "generated", "evolution", "$reader.jsonl"))
            @test length(expected) == length(lines)
            ok = 0
            for (line, exp) in zip(lines, expected)
                v = Avro.fromjson(ev, line)
                got = Avro.decode(ev, Avro.encode(ev, v); reader_schema=rs, union_resolution=:java)
                ok += isequal(got, Avro.fromjson(rs, exp))
            end
            @test ok == length(lines)
        end
        ws = P(read(joinpath(FIXTURES, "generated", "singleobject", "weather.avsc"), String))
        wr = P(read(joinpath(FIXTURES, "generated", "evolution", "weather_reader.avsc"), String))
        wlines = readlines(joinpath(FIXTURES, "apache", "weather.json"))
        wexp = readlines(joinpath(FIXTURES, "generated", "evolution", "weather_reader.jsonl"))
        @test length(wlines) == length(wexp) && !isempty(wlines)
        @test all(isequal(Avro.decode(ws, Avro.encode(ws, Avro.fromjson(ws, l)); reader_schema=wr), Avro.fromjson(wr, e)) for (l, e) in zip(wlines, wexp))
        @test_throws Avro.ResolutionError Avro.resolve(ws, P(read(joinpath(FIXTURES, "generated", "evolution", "weather_reader_fail.avsc"), String)))
    end
    @testset "typed targets and single-object with a reader schema" begin
        w = P("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"string\"}]}")
        r = P("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"b\",\"type\":\"string\"},{\"name\":\"a\",\"type\":\"long\"}]}")
        bytes = Avro.encode(w, (a=7, b="x"))
        @test Avro.decode(w, bytes, NamedTuple{(:b, :a),Tuple{String,Int64}}; reader_schema=r) === (b="x", a=Int64(7))
        store = Avro.SchemaCache()
        Avro.register!(store, w)
        msg = Avro.encodesingle(w, (a=1, b="z"))
        @test Avro.decodesingle(msg, store; reader_schema=r).a === Int64(1)
    end
end
