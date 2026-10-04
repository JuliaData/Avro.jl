@testset "Schema resolution" begin
    P = Avro.parseschema
    function rt(w, r, x; kw...)
        return Avro.decode(w, Avro.encode(w, x); reader_schema=r, kw...)
    end
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

        union_writer = P("[{\"type\":\"record\",\"name\":\"BranchRecord\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}]")
        late_reader = P("{\"type\":\"record\",\"name\":\"BranchRecord\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"z\",\"type\":\"int\"}]}")
        early_reader = P("{\"type\":\"record\",\"name\":\"OtherRecord\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"z\",\"type\":\"int\"}]}")
        branchcharges = Int[]
        for branchreader in (late_reader, early_reader)
            branchbudget = Avro.Budget(Avro.Limits(); available=1 << 40)
            branchplan = Avro.resolve(union_writer, branchreader; budget=branchbudget).plan
            @test branchplan.branches[1] isa Avro.UnresolvableBranch
            failedbranch = branchplan.branches[1]
            diagnostics = Avro.stringbytes(sizeof(failedbranch.msg)) +
                          Avro.stringbytes(sizeof(failedbranch.writerpath)) +
                          Avro.stringbytes(sizeof(failedbranch.readerpath))
            push!(branchcharges, branchbudget.reserved - diagnostics)
            Avro.close!(branchbudget)
        end
        @test branchcharges[1] == branchcharges[2]

        diagnosticcharges = Int[]
        enumdiagnosticcharges = Int[]
        for n in (1, 1_000)
            field = "x"^n
            diagnosticwriter = P("[{\"type\":\"record\",\"name\":\"DiagnosticRecord\",\"fields\":[{\"name\":\"$field\",\"type\":\"string\"}]},\"null\"]")
            diagnosticreader = P("{\"type\":\"record\",\"name\":\"DiagnosticRecord\",\"fields\":[{\"name\":\"$field\",\"type\":\"long\"}]}")
            diagnosticbudget = Avro.Budget(Avro.Limits(); available=1 << 40)
            diagnosticplan = Avro.resolve(diagnosticwriter, diagnosticreader;
                budget=diagnosticbudget).plan
            @test diagnosticplan.branches[1] isa Avro.UnresolvableBranch
            @test diagnosticbudget.pending == 0
            push!(diagnosticcharges, diagnosticbudget.reserved)
            Avro.close!(diagnosticbudget)

            enumwriter = P("{\"type\":\"record\",\"name\":\"EnumDiagnosticRecord\",\"fields\":[{\"name\":\"$field\",\"type\":{\"type\":\"enum\",\"name\":\"DiagnosticEnum\",\"symbols\":[\"A\",\"B\"]}}]}")
            enumreader = P("{\"type\":\"record\",\"name\":\"EnumDiagnosticRecord\",\"fields\":[{\"name\":\"$field\",\"type\":{\"type\":\"enum\",\"name\":\"DiagnosticEnum\",\"symbols\":[\"B\",\"A\"]}}]}")
            enumbudget = Avro.Budget(Avro.Limits(); available=1 << 40)
            enumplan = Avro.resolve(enumwriter, enumreader; budget=enumbudget).plan
            @test enumplan.steps[1].second isa Avro.EnumRemapPlan
            @test enumbudget.pending == 0
            push!(enumdiagnosticcharges, enumbudget.reserved)
            Avro.close!(enumbudget)
        end
        @test diagnosticcharges[2] - diagnosticcharges[1] >= 2 * 999
        @test enumdiagnosticcharges[2] - enumdiagnosticcharges[1] >= 2 * 999

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
        primitive_limits = Avro.Limits(max_bytes=0, max_datum_bytes=1 << 20)
        for mode in (:strict, :fast)
            @test rt(Avro.FloatSchema(), Avro.DoubleSchema(), 1.5f0;
                     validate=mode, limits=primitive_limits) === 1.5
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

        decimalwriter = P("{\"type\":\"fixed\",\"name\":\"a.F\",\"size\":4,\"logicalType\":\"decimal\",\"precision\":8,\"scale\":2}")
        decimalreader = P("[{\"type\":\"fixed\",\"name\":\"b.F\",\"size\":4,\"logicalType\":\"decimal\",\"precision\":9,\"scale\":2},{\"type\":\"fixed\",\"name\":\"a.F\",\"size\":4,\"logicalType\":\"decimal\",\"precision\":8,\"scale\":2}]")
        decimalvalue = Avro.Decimal(1, 2)
        resolveddecimal = rt(decimalwriter, decimalreader, decimalvalue)
        @test resolveddecimal isa Avro.UnionValue && resolveddecimal.index == 2
        uniondecimalwriter = P("[{\"type\":\"fixed\",\"name\":\"a.F\",\"size\":4,\"logicalType\":\"decimal\",\"precision\":8,\"scale\":2},\"null\"]")
        resolveduniondecimal = rt(uniondecimalwriter, decimalreader,
            Avro.UnionValue(1, decimalvalue))
        @test resolveduniondecimal isa Avro.UnionValue && resolveduniondecimal.index == 2

        aliasrecordwriter = P("{\"type\":\"record\",\"name\":\"w.Foo\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}")
        aliasrecordreader = P("[{\"type\":\"record\",\"name\":\"x.Foo\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]},{\"type\":\"record\",\"name\":\"r.Bar\",\"aliases\":[\"w.Foo\"],\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}]")
        specrecord = rt(aliasrecordwriter, aliasrecordreader, (a=Int32(7),))
        javarecord = rt(aliasrecordwriter, aliasrecordreader, (a=Int32(7),); union_resolution=:java)
        @test specrecord.index == 1 && specrecord.value["a"] === Int64(7)
        @test javarecord.index == 2 && javarecord.value["a"] === Int32(7)

        aliasenumwriter = P("{\"type\":\"enum\",\"name\":\"w.Kind\",\"symbols\":[\"A\"]}")
        aliasenumreader = P("[{\"type\":\"enum\",\"name\":\"x.Kind\",\"symbols\":[\"A\"]},{\"type\":\"enum\",\"name\":\"r.Other\",\"aliases\":[\"w.Kind\"],\"symbols\":[\"A\"]}]")
        enumvalue = Avro.EnumValue(aliasenumwriter, 1)
        @test rt(aliasenumwriter, aliasenumreader, enumvalue).index == 1
        @test rt(aliasenumwriter, aliasenumreader, enumvalue; union_resolution=:java).index == 2

        aliasfixedwriter = P("{\"type\":\"fixed\",\"name\":\"w.Blob\",\"size\":2}")
        aliasfixedreader = P("[{\"type\":\"fixed\",\"name\":\"x.Blob\",\"size\":2},{\"type\":\"fixed\",\"name\":\"r.OtherBlob\",\"aliases\":[\"w.Blob\"],\"size\":2}]")
        fixedvalue = Avro.Fixed(aliasfixedwriter, UInt8[1, 2])
        @test rt(aliasfixedwriter, aliasfixedreader, fixedvalue).index == 1
        @test rt(aliasfixedwriter, aliasfixedreader, fixedvalue; union_resolution=:java).index == 2

        softrecordwriter = P("""{"type":"record","name":"w.W","fields":[{"name":"x","type":"int"}]}""")
        softrecordreader = P("""[
            {"type":"record","name":"a.A","fields":[{"name":"x","type":"int"}]},
            {"type":"record","name":"b.B","fields":[{"name":"x","type":"long"}]}
        ]""")
        softrecord = rt(softrecordwriter, softrecordreader, (x=Int32(7),);
                        union_resolution=:java)
        @test softrecord isa Avro.UnionValue
        @test softrecord.index == 1
        @test softrecord.value["x"] === Int32(7)
        nestedsoftwriter = P("""{"type":"record","name":"w.W","fields":[
            {"name":"m","type":{"type":"record","name":"w.WM","fields":[
                {"name":"leaf","type":{"type":"record","name":"w.WL","fields":[
                    {"name":"x","type":"int"}]}}]}}]}""")
        nestedsoftreader = P("""[
            {"type":"record","name":"a.A","fields":[
                {"name":"m","type":{"type":"record","name":"a.AM","fields":[
                    {"name":"leaf","type":{"type":"record","name":"a.AL","fields":[
                        {"name":"y","type":"int"}]}}]}}]},
            {"type":"record","name":"b.B","fields":[
                {"name":"m","type":{"type":"record","name":"w.WM","fields":[
                    {"name":"leaf","type":{"type":"record","name":"w.WL","fields":[
                        {"name":"x","type":"long"}]}}]}}]}]""")
        nestedsoftbytes = Avro.encode(nestedsoftwriter, (m=(leaf=(x=Int32(7),),),))
        @test_throws Avro.ResolutionError Avro.decode(
            nestedsoftwriter, nestedsoftbytes; reader_schema=nestedsoftreader,
            union_resolution=:java)
        shortnamewriter = P("""{"type":"record","name":"w.Target","fields":[
            {"name":"x","type":"int"}]}""")
        shortnamereader = P("""[
            {"type":"record","name":"a.First","fields":[{"name":"x","type":"int"}]},
            {"type":"record","name":"b.Target","fields":[{"name":"x","type":"long"}]}]""")
        shortnamevalue = rt(shortnamewriter, shortnamereader, (x=Int32(9),);
                            union_resolution=:java)
        @test shortnamevalue.index == 2
        @test shortnamevalue.value["x"] === Int64(9)
        immediatewriter = P("""{"type":"record","name":"w.Immediate","fields":[
            {"name":"x","type":"string"}]}""")
        immediatereader = P("""[
            {"type":"record","name":"a.Bad","fields":[{"name":"x","type":"int"}]},
            {"type":"record","name":"b.Good","fields":[{"name":"x","type":"bytes"}]}]""")
        immediatevalue = rt(immediatewriter, immediatereader, (x="ok",);
                            union_resolution=:java)
        @test immediatevalue.index == 2
        @test immediatevalue.value["x"] == UInt8[0x6f, 0x6b]
    end
    @testset "work limit, repair, equality shortcut" begin
        function wide(pfx, n)
            return P("[" * join(["{\"type\":\"record\",\"name\":\"$(pfx)$i\",\"fields\":[]}" for i in 1:n], ",") * "]")
        end
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
    @testset "reader defaults are admitted before zero-input materialisation" begin
        nestedwriter = P("""{"type":"record","name":"W","fields":[]}""")
        nestedreader = P("""{"type":"record","name":"W","fields":[
            {"name":"outer","type":{"type":"record","name":"DefaultOuter","fields":[
                {"name":"u","type":["null","int"],"default":7}]},"default":{}}]}""")
        nesteddefault = Avro.resolve(nestedwriter, nestedreader).plan.defaults[1].second
        @test nesteddefault.values == 3
        @test nesteddefault.branches == [2]
        directlimits = Avro.Limits(max_total_values=3, work_allowance=3)
        directbudget = Avro.Budget(directlimits; available=1 << 30)
        directvalue = Avro.jsonvalue(nesteddefault, directbudget)
        @test directvalue.u === Int32(7)
        @test directbudget.values == 3
        Avro.close!(directbudget)
        nestedlimits = Avro.Limits(max_total_values=4)
        nestedtarget = @NamedTuple{outer::@NamedTuple{u::Union{Missing,Int32}}}
        nestedcontainer = take!(Avro.tobuffer([NamedTuple()]; schema=nestedwriter))
        for mode in (:strict, :fast)
            nestedvalue = Avro.decode(nestedwriter, UInt8[]; reader_schema=nestedreader,
                                      limits=nestedlimits, validate=mode)
            @test nestedvalue.outer.u === Int32(7)
            nestedprepared = Avro.DatumReader(nestedwriter; reader_schema=nestedreader,
                                              limits=nestedlimits, validate=mode)
            @test nestedprepared(UInt8[]).outer.u === Int32(7)
            nestedtyped = Avro.DatumReader(nestedwriter, nestedtarget;
                                           reader_schema=nestedreader,
                                           limits=nestedlimits, validate=mode)
            @test nestedtyped(UInt8[]) == (outer=(u=Int32(7),),)
            @test Avro.decode(nestedwriter, UInt8[], nestedtarget;
                              reader_schema=nestedreader, limits=nestedlimits,
                              validate=mode) == (outer=(u=Int32(7),),)
            nestedtable = Avro.Table(nestedcontainer; reader_schema=nestedreader,
                                     select=(:outer,), validate=mode, ntasks=1)
            @test Tables.getcolumn(nestedtable, :outer)[1].u === Int32(7)
        end

        depthwriter = P("""{"type":"record","name":"DepthRow","fields":[]}""")
        depthreader = P("""{"type":"record","name":"DepthRow","fields":[
            {"name":"child","type":{"type":"record","name":"DepthChild",
             "fields":[]},"default":{}}]}""")
        depthbytes = Avro.encode(depthwriter, NamedTuple())
        depthlimits = Avro.Limits(max_depth=1)
        DepthChild = @NamedTuple{}
        DepthRow = @NamedTuple{child::DepthChild}
        function defaulterror(f)
            err = try
                f()
                nothing
            catch caught
                caught
            end
            @test err isa Avro.LimitError
            if err isa Avro.LimitError
                @test err.limit === :max_depth
                @test err.observed == 2
                @test err.value == 1
            end
            return err
        end
        depthcontainer = take!(Avro.tobuffer([NamedTuple()]; schema=depthwriter,
                                              codec=:null))
        for mode in (:strict, :fast)
            defaulterror(() -> Avro.decode(
                depthwriter, depthbytes; reader_schema=depthreader,
                limits=depthlimits, validate=mode))
            defaulterror(() -> Avro.DatumReader(
                depthwriter; reader_schema=depthreader,
                limits=depthlimits, validate=mode)(depthbytes))
            defaulterror(() -> Avro.decode(
                depthwriter, depthbytes, DepthRow; reader_schema=depthreader,
                limits=depthlimits, validate=mode))
            defaulterror(() -> Avro.DatumReader(
                depthwriter, DepthRow; reader_schema=depthreader,
                limits=depthlimits, validate=mode)(depthbytes))
            defaulterror(() -> collect(Avro.Rows(
                depthcontainer; reader_schema=depthreader,
                limits=depthlimits, validate=mode)))
            defaulterror(() -> collect(Avro.Rows(
                depthcontainer; T=DepthRow, reader_schema=depthreader,
                limits=depthlimits, validate=mode)))
            defaulterror(() -> Avro.Table(
                depthcontainer; reader_schema=depthreader,
                limits=depthlimits, validate=mode, ntasks=1))
        end
        @test Avro.decode(depthwriter, depthbytes;
                          reader_schema=depthreader,
                          limits=Avro.Limits(max_depth=2)).child ==
              Avro.Record(depthreader.fields[1].schema, Any[])
        nestederror = try
            Avro.decode(nestedwriter, UInt8[]; reader_schema=nestedreader,
                        limits=Avro.Limits(max_total_values=3))
            nothing
        catch caught
            caught
        end
        @test nestederror isa Avro.LimitError
        @test nestederror.limit === :max_total_values
        @test nestederror.observed == 4
        @test nestederror.value == 3
        ownedreader = P("""{"type":"record","name":"W","fields":[
            {"name":"scalar","type":"string","default":"scalar-default"},
            {"name":"nested","type":{"type":"array","items":"string"},
             "default":["nested-default"]},
            {"name":"map","type":{"type":"map","values":"string"},
             "default":{"map-key":"map-value"}}]}""")
        ownedplan = Avro.resolve(nestedwriter, ownedreader).plan
        mapdefault = ownedplan.defaults[3].second
        mapbudget = Avro.Budget(Avro.Limits(); available=1 << 30)
        directmap = Avro.jsonvalue(mapdefault, mapbudget)
        @test mapbudget.reserved == Avro.storagebytes(directmap)
        frozenmap = mapdefault.json
        frozenkey = first(frozenmap.order)
        outputkey = first(keys(directmap))
        GC.@preserve frozenkey outputkey begin
            @test pointer(frozenkey) != pointer(outputkey)
        end
        frozenvalue = frozenmap.members[frozenkey]
        outputvalue = directmap["map-key"]
        GC.@preserve frozenvalue outputvalue begin
            @test pointer(frozenvalue) != pointer(outputvalue)
        end
        Avro.close!(mapbudget)

        function distinctstringstorage(first::String, second::String)
            GC.@preserve first second begin
                return pointer(first) != pointer(second)
            end
        end
        function checkowneddefaults(left, right)
            @test left.scalar == right.scalar == "scalar-default"
            @test left.nested == right.nested == ["nested-default"]
            @test left.map == right.map == Avro.Map([("map-key", "map-value")])
            @test distinctstringstorage(left.scalar, right.scalar)
            @test distinctstringstorage(left.nested[1], right.nested[1])
            @test distinctstringstorage(first(keys(left.map)), first(keys(right.map)))
            @test distinctstringstorage(left.map["map-key"], right.map["map-key"])
            return nothing
        end
        ownedprepared = Avro.DatumReader(nestedwriter; reader_schema=ownedreader)
        checkowneddefaults(ownedprepared(UInt8[]), ownedprepared(UInt8[]))
        checkowneddefaults(
            Avro.decode(nestedwriter, UInt8[]; reader_schema=ownedreader),
            Avro.decode(nestedwriter, UInt8[]; reader_schema=ownedreader))
        OwnedDefaults = @NamedTuple{
            scalar::String, nested::Vector{String}, map::Avro.Map{String}}
        ownedtyped = Avro.DatumReader(nestedwriter, OwnedDefaults;
                                      reader_schema=ownedreader)
        checkowneddefaults(ownedtyped(UInt8[]), ownedtyped(UInt8[]))
        ownedcontainer = take!(Avro.tobuffer([NamedTuple(), NamedTuple()];
                                             schema=nestedwriter, codec=:null))
        Avro.Rows(ownedcontainer; reader_schema=ownedreader) do rows
            values = collect(rows)
            @test length(values) == 2
            checkowneddefaults(values[1], values[2])
        end

        largedefault = repeat("x", 100_000)
        largeschemalimits = Avro.Limits(max_bytes=1_000_000,
                                        max_datum_bytes=1_000_000,
                                        max_schema_bytes=200_000)
        largereader = P("{\"type\":\"record\",\"name\":\"W\",\"fields\":[{\"name\":\"s\",\"type\":\"string\",\"default\":\"" *
                        largedefault * "\"}]}", limits=largeschemalimits)
        smalllimits = Avro.Limits(max_bytes=1024, max_datum_bytes=1024,
                                  max_block_bytes=1024,
                                  max_block_output_bytes=1024,
                                  max_metadata_bytes=0, max_schema_bytes=0)
        largeprepared = Avro.DatumReader(nestedwriter;
                                         reader_schema=largereader,
                                         limits=smalllimits)
        largeerror = try
            largeprepared(UInt8[])
            nothing
        catch caught
            caught
        end
        @test largeerror isa Avro.LimitError
        @test largeerror.limit === :max_bytes
        @test largeerror.observed == sizeof(largedefault)
        compoundreader = P("""{"type":"record","name":"W","fields":[
            {"name":"array_default","type":{"type":"array","items":["null","int"]},
             "default":[7]},
            {"name":"map_default","type":{"type":"map","values":["null","int"]},
             "default":{"k":7}},
            {"name":"present_default","type":{"type":"record","name":"PresentDefault",
             "fields":[{"name":"u","type":["null","int"]}]},"default":{"u":7}},
            {"name":"omitted_default","type":{"type":"record","name":"OmittedDefault",
             "fields":[{"name":"u","type":["null","int"],"default":7}]},
             "default":{}}]}""")
        compoundplan = Avro.resolve(nestedwriter, compoundreader).plan
        @test [dp.values for (_, dp) in compoundplan.defaults] == fill(3, 4)
        @test [dp.branches for (_, dp) in compoundplan.defaults] == fill([2], 4)
        for (_, dp) in compoundplan.defaults
            budget = Avro.Budget(Avro.Limits(max_total_values=3);
                                 available=1 << 30)
            Avro.jsonvalue(dp, budget)
            @test budget.values == 3
            Avro.close!(budget)
        end
        compoundtarget = @NamedTuple{
            array_default::Vector{Union{Missing,Int32}},
            map_default::Dict{String,Union{Missing,Int32}},
            present_default::@NamedTuple{u::Union{Missing,Int32}},
            omitted_default::@NamedTuple{u::Union{Missing,Int32}}}
        function checkcompound(value)
            @test value.array_default == Union{Missing,Int32}[Int32(7)]
            @test length(value.map_default) == 1
            @test value.map_default["k"] === Int32(7)
            @test value.present_default.u === Int32(7)
            @test value.omitted_default.u === Int32(7)
            return nothing
        end
        compoundlimits = Avro.Limits(max_total_values=13)
        for mode in (:strict, :fast)
            checkcompound(Avro.decode(nestedwriter, UInt8[];
                                      reader_schema=compoundreader,
                                      limits=compoundlimits, validate=mode))
            preparedgeneric = Avro.DatumReader(nestedwriter;
                                               reader_schema=compoundreader,
                                               limits=compoundlimits, validate=mode)
            checkcompound(preparedgeneric(UInt8[]))
            checkcompound(Avro.decode(nestedwriter, UInt8[], compoundtarget;
                                      reader_schema=compoundreader,
                                      limits=compoundlimits, validate=mode))
            preparedtyped = Avro.DatumReader(nestedwriter, compoundtarget;
                                             reader_schema=compoundreader,
                                             limits=compoundlimits, validate=mode)
            checkcompound(preparedtyped(UInt8[]))
        end
        function totalvalueerror(f)
            err = try
                f()
                nothing
            catch caught
                caught
            end
            @test err isa Avro.LimitError
            @test err.limit === :max_total_values
            @test err.observed == 13
            @test err.value == 12
            return err
        end
        shortlimits = Avro.Limits(max_total_values=12)
        totalvalueerror(() -> Avro.decode(nestedwriter, UInt8[];
                                         reader_schema=compoundreader,
                                         limits=shortlimits))
        totalvalueerror(() -> Avro.decode(nestedwriter, UInt8[], compoundtarget;
                                         reader_schema=compoundreader,
                                         limits=shortlimits))
        totalvalueerror(() -> Avro.DatumReader(
            nestedwriter; reader_schema=compoundreader,
            limits=shortlimits)(UInt8[]))
        totalvalueerror(() -> Avro.DatumReader(
            nestedwriter, compoundtarget; reader_schema=compoundreader,
            limits=shortlimits)(UInt8[]))
        compoundio = IOBuffer()
        compoundwriter = Avro.Writer(compoundio, nestedwriter)
        for _ in 1:4
            push!(compoundwriter, NamedTuple())
            flush(compoundwriter)
        end
        close(compoundwriter)
        compoundcontainer = take!(compoundio)
        for mode in (:strict, :fast)
            Avro.Rows(compoundcontainer; reader_schema=compoundreader,
                      validate=mode) do rows
                values = collect(rows)
                @test length(values) == 4
                foreach(checkcompound, values)
            end
            Avro.Rows(compoundcontainer; reader_schema=compoundreader,
                      select=(:array_default,), validate=mode) do rows
                values = collect(rows)
                @test length(values) == 4
                @test all(value -> value.array_default ==
                          Union{Missing,Int32}[Int32(7)], values)
            end
            compoundtable = Avro.Table(compoundcontainer;
                                       reader_schema=compoundreader,
                                       validate=mode, ntasks=1)
            @test length(compoundtable) == 4
            checkcompound(first(Tables.rows(compoundtable)))
            if Threads.nthreads() > 1
                paralleltable = Avro.Table(compoundcontainer;
                                           reader_schema=compoundreader,
                                           validate=mode, ntasks=2)
                @test length(paralleltable) == 4
                checkcompound(first(Tables.rows(paralleltable)))
            end
        end
        writer = P("""{"type":"record","name":"W","fields":[
            {"name":"payload","type":"bytes"}]}""")
        defaults = join(fill("null", 1000), ",")
        reader = P("""{"type":"record","name":"W","fields":[
            {"name":"added","type":{"type":"array","items":"null"},
             "default":[$defaults]}]}""")
        datumlimits = Avro.Limits(max_values_per_byte=16, work_allowance=100)
        containerlimits = Avro.Limits(max_values_per_byte=16, work_allowance=10)
        datum = Avro.encode(writer, (payload=UInt8[],))
        function workerror(f, expectedlimit)
            err = try
                f()
                nothing
            catch e
                e
            end
            @test err isa Avro.LimitError
            @test err.limit === :max_values_per_byte
            @test err.observed == 1003
            @test err.value == expectedlimit
            return err
        end
        workerror(() -> Avro.decode(writer, datum; reader_schema=reader,
                                    limits=datumlimits), 116)
        prepared = Avro.DatumReader(writer; reader_schema=reader,
                                    limits=datumlimits)
        workerror(() -> prepared(datum), 116)
        store = Avro.SchemaCache()
        Avro.register!(store, writer)
        message = Avro.encodesingle(writer, (payload=UInt8[],))
        workerror(() -> Avro.decodesingle(message, store; reader_schema=reader,
                                          limits=datumlimits), 116)

        io = IOBuffer()
        containerwriter = Avro.Writer(io, writer; block_bytes=1 << 20)
        push!(containerwriter, (payload=UInt8[],))
        push!(containerwriter, (payload=zeros(UInt8, 10_000),))
        close(containerwriter)
        container = take!(io)
        workerror(() -> iterate(Avro.Rows(container; reader_schema=reader,
                                          limits=containerlimits)), 26)
        workerror(() -> iterate(Tables.partitions(Avro.Rows(
            container; reader_schema=reader, limits=containerlimits))), 26)
        workerror(() -> Tables.columns(Avro.Rows(
            container; reader_schema=reader, limits=containerlimits)), 26)
        typed = NamedTuple{(:added,),Tuple{Vector{Missing}}}
        workerror(() -> iterate(Avro.Rows(
            container; T=typed, reader_schema=reader,
            limits=containerlimits)), 26)
        workerror(() -> Avro.Table(container; reader_schema=reader,
                                  limits=containerlimits, ntasks=1), 26)
        workerror(() -> Avro.Table(IOBuffer(container); reader_schema=reader,
                                  limits=containerlimits, ntasks=1), 26)
        if Threads.nthreads() > 1
            workerror(() -> Avro.Table(container; reader_schema=reader,
                                      limits=containerlimits, ntasks=2), 26)
        end
        selected = Avro.Rows(container; reader_schema=reader, select=(:added,),
                             limits=containerlimits)
        workerror(() -> iterate(selected), 26)
        unselected = Avro.Rows(container; reader_schema=reader, select=Symbol[],
                               limits=containerlimits)
        @test count(Returns(true), unselected) == 2
        @test length(Avro.Table(container; reader_schema=reader, select=Symbol[],
                                limits=containerlimits, ntasks=1)) == 2

        hugewriter = P("""{"type":"array","items":{"type":"record",
            "name":"HugeDefault","fields":[]}}""")
        hugereader = P("""{"type":"array","items":{"type":"record",
            "name":"HugeDefault","fields":[
            {"name":"d","type":"null","default":null}]}}""")
        hugebytes = vcat(Avro.encode(Avro.LongSchema(), typemax(Int64) - 1),
                         UInt8[0])
        hugelimits = Avro.Limits(max_block_count=typemax(Int),
                                 max_total_values=typemax(Int),
                                 max_values_per_byte=typemax(Int))
        hugeerr = try
            Avro.decode(hugewriter, hugebytes; reader_schema=hugereader,
                        limits=hugelimits)
            nothing
        catch caught
            caught
        end
        @test hugeerr isa Avro.LimitError
        @test hugeerr.limit === :max_total_values
        @test hugeerr.observed === typemax(Int) # the first default fails before the huge loop
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
