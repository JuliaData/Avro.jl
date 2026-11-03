@testset "Schema resolution" begin
    P = Avro.parseschema
    function rt(w, r, x; kw...)
        return Avro.decode(w, Avro.encode(w, x); reader_schema=r, kw...)
    end
    @testset "resolve, policies, ResolvedSchema" begin
        w = P("\"int\"")
        rs = Avro.resolve(w, P("\"long\""))
        @test rs isa Avro.ResolvedSchema && rs.union_resolution === :spec && rs.plan isa Avro.PromotePlan
        array_writer = P("{\"type\":\"array\",\"items\":\"int\"}")
        array_reader = P("{\"type\":\"array\",\"items\":\"long\"}")
        @test Avro.resolve(array_writer, array_reader).plan isa Avro.ArrayPlan

        union_writer = P("[{\"type\":\"record\",\"name\":\"BranchRecord\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}]")
        late_reader = P("{\"type\":\"record\",\"name\":\"BranchRecord\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"z\",\"type\":\"int\"}]}")
        early_reader = P("{\"type\":\"record\",\"name\":\"OtherRecord\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"z\",\"type\":\"int\"}]}")
        for branchreader in (late_reader, early_reader)
            @test Avro.resolve(union_writer, branchreader).plan.branches[1] isa Avro.UnresolvableBranch
        end

        for n in (1, 1_000)
            field = "x"^n
            diagnosticwriter = P("[{\"type\":\"record\",\"name\":\"DiagnosticRecord\",\"fields\":[{\"name\":\"$field\",\"type\":\"string\"}]},\"null\"]")
            diagnosticreader = P("{\"type\":\"record\",\"name\":\"DiagnosticRecord\",\"fields\":[{\"name\":\"$field\",\"type\":\"long\"}]}")
            @test Avro.resolve(diagnosticwriter, diagnosticreader).plan.branches[1] isa Avro.UnresolvableBranch
            enumwriter = P("{\"type\":\"record\",\"name\":\"EnumDiagnosticRecord\",\"fields\":[{\"name\":\"$field\",\"type\":{\"type\":\"enum\",\"name\":\"DiagnosticEnum\",\"symbols\":[\"A\",\"B\"]}}]}")
            enumreader = P("{\"type\":\"record\",\"name\":\"EnumDiagnosticRecord\",\"fields\":[{\"name\":\"$field\",\"type\":{\"type\":\"enum\",\"name\":\"DiagnosticEnum\",\"symbols\":[\"B\",\"A\"]}}]}")
            @test Avro.resolve(enumwriter, enumreader).plan.steps[1].second isa Avro.EnumRemapPlan
        end

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
        @test rt(Avro.FloatSchema(), Avro.DoubleSchema(), 1.5f0) === 1.5
        for (wk, rk) in (("\"long\"", "\"int\""), ("\"double\"", "\"float\""), ("\"double\"", "\"long\""), ("\"string\"", "\"int\""), ("\"boolean\"", "\"int\""), ("\"null\"", "\"boolean\""))
            @test_throws Avro.ResolutionError Avro.resolve(P(wk), P(rk))
        end
        @test_throws Avro.DataError rt(P("\"bytes\""), P("\"string\""), UInt8[0xff])   # bytes→string validates UTF-8
    end
    @testset "logical pairings: the reader's interpretation wins over the raw value" begin
        @test rt(P("{\"type\":\"int\",\"logicalType\":\"date\"}"), P("\"int\""), Date(1970, 1, 3)) === Int32(2)
        @test rt(P("\"long\""), P("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}"), Int64(1500)) === Avro.timestamp(Millisecond, 1500)
        @test rt(P("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}"), P("{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}"), Avro.timestamp(Millisecond, 7)) ===
              Avro.timestamp(Microsecond, 7)                                   # no unit conversion (documented hazard)
        @test rt(P("{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}"), P("{\"type\":\"long\",\"logicalType\":\"local-timestamp-micros\"}"), Avro.timestamp(Microsecond, 9)) ===
              Avro.localtimestamp(Microsecond, 9)                              # global → local reinterprets
        @test rt(P("{\"type\":\"int\",\"logicalType\":\"date\"}"), P("{\"type\":\"int\",\"logicalType\":\"time-millis\"}"), Date(1970, 1, 2)) === Time(0, 0, 0, 1)
        @test rt(P("\"int\""), P("{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}"), Int32(4)) === Avro.timestamp(Microsecond, 4)
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
        @test keys(v) == [:b, :a, :c] && v.a === Int64(7) && v.b == "x" && v.c == Int32[1, 2]
        v2 = rt(w, r, (a=8, b="y"))
        push!(getfield(v, :values)[3], Int32(9))
        @test v2.c == Int32[1, 2]                                              # defaults are fresh per record
        rnodefault = P("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"z\",\"type\":\"int\"}]}")
        @test_throws Avro.ResolutionError Avro.resolve(w, rnodefault)
        # writer-only fields are skipped
        @test rt(w, P("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"b\",\"type\":\"string\"}]}"), (a=1, b="q")).b == "q"
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
        # union values decode to the bare value of the reader branch, so its Julia type shows the branch
        @test rt(wi, P("[\"long\",\"int\"]"), Int32(5)) === Int64(5)                                          # first matching reader branch
        @test rt(wi, P("[\"long\",\"int\"]"), Int32(5); union_resolution=:java) === Int32(5)
        @test rt(wi, P("[\"double\",\"int\"]"), Int32(5)) === 5.0
        @test rt(wi, P("[\"double\",\"int\"]"), Int32(5); union_resolution=:java) === Int32(5)
        @test rt(wi, P("[\"string\",\"double\",\"long\"]"), Int32(5)) === 5.0                                 # else the first promotion
        big = typemax(Int64) - 1                                                                               # not exact in a double
        @test rt(P("\"long\""), P("[\"double\",\"long\"]"), big) === Float64(big)
        @test rt(P("\"long\""), P("[\"double\",\"long\"]"), big; union_resolution=:java) === big
        @test rt(wi, P("[\"null\",\"long\"]"), Int32(5)) === Int64(5)                                          # nullable readers stay bare
        @test_throws Avro.ResolutionError Avro.resolve(wi, P("[\"string\",\"boolean\"]"))
        wu = P("[\"int\",\"string\"]")
        @test rt(wu, P("\"long\""), Avro.UnionValue(1, Int32(9))) === Int64(9)
        @test_throws Avro.ResolutionError rt(wu, P("\"long\""), Avro.UnionValue(2, "x"))                       # raised only when the datum selects it
        wn = P("[\"null\",\"int\"]")
        @test rt(wn, P("\"int\""), Int32(3)) === Int32(3)
        @test_throws Avro.ResolutionError rt(wn, P("\"int\""), missing)
        @test rt(wu, P("[\"string\",\"long\"]"), Avro.UnionValue(1, Int32(9))) === Int64(9)
        @test rt(wu, P("[\"string\",\"long\"]"), Avro.UnionValue(2, "x")) == "x"
        @test rt(P("[\"null\",\"string\"]"), P("[\"string\",\"null\"]"), missing) === missing
        @test rt(P("[\"null\",\"string\"]"), P("[\"string\",\"null\"]"), "q") == "q"
        @test_throws Avro.ResolutionError rt(P("[\"int\",\"boolean\"]"), P("\"long\""), Avro.UnionValue(2, true))
        @test rt(P("[\"int\",\"boolean\"]"), P("\"long\""), Avro.UnionValue(1, Int32(2))) === Int64(2)

        decimalwriter = P("{\"type\":\"fixed\",\"name\":\"a.F\",\"size\":4,\"logicalType\":\"decimal\",\"precision\":8,\"scale\":2}")
        decimalreader = P("[{\"type\":\"fixed\",\"name\":\"b.F\",\"size\":4,\"logicalType\":\"decimal\",\"precision\":9,\"scale\":2},{\"type\":\"fixed\",\"name\":\"a.F\",\"size\":4,\"logicalType\":\"decimal\",\"precision\":8,\"scale\":2}]")
        decimalvalue = Avro.Decimal(1, 2)
        @test rt(decimalwriter, decimalreader, decimalvalue) == decimalvalue
        uniondecimalwriter = P("[{\"type\":\"fixed\",\"name\":\"a.F\",\"size\":4,\"logicalType\":\"decimal\",\"precision\":8,\"scale\":2},\"null\"]")
        @test rt(uniondecimalwriter, decimalreader, Avro.UnionValue(1, decimalvalue)) == decimalvalue

        aliasrecordwriter = P("{\"type\":\"record\",\"name\":\"w.Foo\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}")
        aliasrecordreader = P("[{\"type\":\"record\",\"name\":\"x.Foo\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]},{\"type\":\"record\",\"name\":\"r.Bar\",\"aliases\":[\"w.Foo\"],\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}]")
        specrecord = rt(aliasrecordwriter, aliasrecordreader, (a=Int32(7),))
        javarecord = rt(aliasrecordwriter, aliasrecordreader, (a=Int32(7),); union_resolution=:java)
        @test Avro.fullname(Avro.schema(specrecord)) == "x.Foo" && specrecord["a"] === Int64(7)     # the earlier unqualified name matches
        @test Avro.fullname(Avro.schema(javarecord)) == "r.Bar" && javarecord["a"] === Int32(7)

        aliasenumwriter = P("{\"type\":\"enum\",\"name\":\"w.Kind\",\"symbols\":[\"A\"]}")
        aliasenumreader = P("[{\"type\":\"enum\",\"name\":\"x.Kind\",\"symbols\":[\"A\"]},{\"type\":\"enum\",\"name\":\"r.Other\",\"aliases\":[\"w.Kind\"],\"symbols\":[\"A\"]}]")
        enumvalue = Avro.EnumValue(aliasenumwriter, 1)
        @test Avro.fullname(rt(aliasenumwriter, aliasenumreader, enumvalue).schema) == "x.Kind"
        @test Avro.fullname(rt(aliasenumwriter, aliasenumreader, enumvalue; union_resolution=:java).schema) == "r.Other"

        aliasfixedwriter = P("{\"type\":\"fixed\",\"name\":\"w.Blob\",\"size\":2}")
        aliasfixedreader = P("[{\"type\":\"fixed\",\"name\":\"x.Blob\",\"size\":2},{\"type\":\"fixed\",\"name\":\"r.OtherBlob\",\"aliases\":[\"w.Blob\"],\"size\":2}]")
        fixedvalue = Avro.Fixed(aliasfixedwriter, UInt8[1, 2])
        @test Avro.fullname(rt(aliasfixedwriter, aliasfixedreader, fixedvalue).schema) == "x.Blob"
        @test Avro.fullname(rt(aliasfixedwriter, aliasfixedreader, fixedvalue; union_resolution=:java).schema) == "r.OtherBlob"

        # :spec takes the first matching unqualified name; :java prefers an exact fullname.
        event(ns, t="int") = """{"type":"record","name":"Event","namespace":"$ns","fields":[{"name":"x","type":"$t"}]}"""
        eventreader = P("[" * event("com.a", "long") * "," * event("com.b") * "]")
        exactevent = rt(P(event("com.b")), eventreader, (x=Int32(7),))
        @test Avro.fullname(Avro.schema(exactevent)) == "com.a.Event" && exactevent["x"] === Int64(7)
        javaevent = rt(P(event("com.b")), eventreader, (x=Int32(7),); union_resolution=:java)
        @test Avro.fullname(Avro.schema(javaevent)) == "com.b.Event" && javaevent["x"] === Int32(7)
        color(ns) = """{"type":"enum","name":"Color","namespace":"$ns","symbols":["R","G"]}"""
        colorwriter = P(color("com.b"))
        colorvalue = rt(colorwriter, P("[" * color("com.a") * "," * color("com.b") * "]"),
                        Avro.EnumValue(colorwriter, 2))
        @test Avro.fullname(colorvalue.schema) == "com.a.Color" && String(colorvalue) == "G"
        firstevent = rt(P(event("com.c")), eventreader, (x=Int32(7),))
        @test Avro.fullname(Avro.schema(firstevent)) == "com.a.Event" && firstevent["x"] === Int64(7)
        @test rt(P(event("com.c")), P("[\"null\"," * event("com.a", "long") * "]"), (x=Int32(3),))["x"] === Int64(3)
        unionwriter = P("[\"null\"," * event("com.c") * "]")
        nullablereader = P("[\"null\"," * event("com.a", "long") * "," * event("com.b") * "]")
        @test Avro.resolve(unionwriter, nullablereader) isa Avro.ResolvedSchema
        @test rt(unionwriter, nullablereader, missing) === missing
        value = rt(unionwriter, nullablereader, (x=Int32(1),))
        @test Avro.fullname(Avro.schema(value)) == "com.a.Event" && value["x"] === Int64(1)

        softrecordwriter = P("""{"type":"record","name":"w.W","fields":[{"name":"x","type":"int"}]}""")
        softrecordreader = P("""[
            {"type":"record","name":"a.A","fields":[{"name":"x","type":"int"}]},
            {"type":"record","name":"b.B","fields":[{"name":"x","type":"long"}]}
        ]""")
        softrecord = rt(softrecordwriter, softrecordreader, (x=Int32(7),);
                        union_resolution=:java)
        @test softrecord isa Avro.Record
        @test Avro.fullname(Avro.schema(softrecord)) == "a.A"
        @test softrecord["x"] === Int32(7)
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
        @test Avro.fullname(Avro.schema(shortnamevalue)) == "b.Target"
        @test shortnamevalue["x"] === Int64(9)
        immediatewriter = P("""{"type":"record","name":"w.Immediate","fields":[
            {"name":"x","type":"string"}]}""")
        immediatereader = P("""[
            {"type":"record","name":"a.Bad","fields":[{"name":"x","type":"int"}]},
            {"type":"record","name":"b.Good","fields":[{"name":"x","type":"bytes"}]}]""")
        immediatevalue = rt(immediatewriter, immediatereader, (x="ok",);
                            union_resolution=:java)
        @test Avro.fullname(Avro.schema(immediatevalue)) == "b.Good"
        @test immediatevalue["x"] == UInt8[0x6f, 0x6b]
    end
    @testset "first union matches across prepared, typed and container readers" begin
        writer = P("\"int\"")
        reader = P("[\"long\",\"int\"]")
        bytes = Avro.encode(writer, Int32(7))
        @test Avro.DatumReader(writer; reader_schema=reader)(bytes) === Int64(7)
        @test Avro.decode(writer, bytes, Union{Int32,Int64}; reader_schema=reader) === Int64(7)
        @test Avro.decode(writer, bytes, Union{Int32,Int64}; reader_schema=reader,
                          union_resolution=:java) === Int32(7)
        recordwriter = P("""{"type":"record","name":"R","fields":[{"name":"n","type":"int"}]}""")
        recordreader = P("""{"type":"record","name":"R","fields":[{"name":"n","type":["long","int"]}]}""")
        buffer = IOBuffer()
        Avro.write(buffer, [(n=Int32(7),)]; schema=recordwriter)
        data = take!(buffer)
        @test only(Avro.Rows(data; reader_schema=recordreader)).n === Int64(7)
        resolvedtable = Avro.Table(data; reader_schema=recordreader)
        resolvedcolumn = Tables.getcolumn(resolvedtable, :n)
        @test resolvedcolumn isa Vector{Any} && only(resolvedcolumn) === Int64(7)
        @test collect(Tables.schema(resolvedtable).types) == [Any]       # a non-nullable union column holds bare values

        # Once a named branch matches, failure while resolving its fields does not try a later branch.
        namedwriter = P("""{"type":"record","name":"w.R","fields":[{"name":"n","type":"int"}]}""")
        namedreader = P("""[
            {"type":"record","name":"a.R","fields":[{"name":"n","type":"boolean"}]},
            {"type":"record","name":"w.R","fields":[{"name":"n","type":"int"}]}]""")
        @test_throws Avro.ResolutionError Avro.resolve(namedwriter, namedreader)
        @test Avro.fullname(Avro.schema(rt(namedwriter, namedreader, (n=Int32(7),); union_resolution=:java))) == "w.R"
    end
    @testset "a failed branch inside a recursive record keeps the record's cached plan" begin
        w = P("""{"type":"record","name":"R","fields":[{"name":"a","type":["null",
            {"type":"record","name":"X","fields":[{"name":"v","type":"string"}]},"R"]}]}""")
        r = P("""{"type":"record","name":"R","fields":[{"name":"a","type":["null",
            {"type":"record","name":"X","fields":[{"name":"v","type":"int"}]},"R"]}]}""")
        @test Avro.resolve(w, r) isa Avro.ResolvedSchema
        recursive = rt(w, r, (a=Avro.UnionValue(3, (a=Avro.UnionValue(1, missing),)),)).a
        @test Avro.fullname(Avro.schema(recursive)) == "R" && recursive.a === missing
        @test_throws Avro.ResolutionError rt(w, r, (a=Avro.UnionValue(2, (v="s",)),))
    end

    @testset "wide records resolve under default limits" begin
        widerecord(t) = P("{\"type\":\"record\",\"name\":\"W\",\"fields\":[" *
                          join(("{\"name\":\"f$i\",\"type\":\"$t\"}" for i in 1:3000), ",") * "]}")
        w, r = widerecord("int"), widerecord("long")
        @test Avro.resolve(w, r) isa Avro.ResolvedSchema
        @test rt(w, r, Dict("f$i" => Int32(i) for i in 1:3000)).f3000 === Int64(3000)
    end

    @testset "repair, equality shortcut" begin
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
    @testset "reader defaults: materialised per record, uncounted against the file's limits" begin
        nestedwriter = P("""{"type":"record","name":"W","fields":[]}""")
        nestedreader = P("""{"type":"record","name":"W","fields":[
            {"name":"outer","type":{"type":"record","name":"DefaultOuter","fields":[
                {"name":"u","type":["null","int"],"default":7}]},"default":{}}]}""")
        nesteddefault = Avro.resolve(nestedwriter, nestedreader).plan.defaults[1].second
        @test Avro.freshvalue(nesteddefault.value).u === Int32(7)
        nestedtarget = @NamedTuple{outer::@NamedTuple{u::Union{Missing,Int32}}}
        nestedcontainer = take!(Avro.tobuffer([NamedTuple()]; schema=nestedwriter))
        @test Avro.decode(nestedwriter, UInt8[]; reader_schema=nestedreader).outer.u === Int32(7)
        @test Avro.DatumReader(nestedwriter; reader_schema=nestedreader)(UInt8[]).outer.u === Int32(7)
        @test Avro.DatumReader(nestedwriter, nestedtarget; reader_schema=nestedreader)(UInt8[]) == (outer=(u=Int32(7),),)
        @test Avro.decode(nestedwriter, UInt8[], nestedtarget; reader_schema=nestedreader) == (outer=(u=Int32(7),),)
        nestedtable = Avro.Table(nestedcontainer; reader_schema=nestedreader, select=(:outer,), ntasks=1)
        @test Tables.getcolumn(nestedtable, :outer)[1].u === Int32(7)

        ownedreader = P("""{"type":"record","name":"W","fields":[
            {"name":"scalar","type":"string","default":"scalar-default"},
            {"name":"nested","type":{"type":"array","items":"string"},
             "default":["nested-default"]},
            {"name":"map","type":{"type":"map","values":"string"},
             "default":{"map-key":"map-value"}}]}""")
        # every record gets its own mutable default containers
        function checkfreshdefaults(left, right)
            @test left.scalar == right.scalar == "scalar-default"
            @test left.nested == right.nested == ["nested-default"]
            @test left.map == right.map == Dict("map-key" => "map-value")
            @test left.nested !== right.nested
            @test left.map !== right.map
            return nothing
        end
        ownedprepared = Avro.DatumReader(nestedwriter; reader_schema=ownedreader)
        checkfreshdefaults(ownedprepared(UInt8[]), ownedprepared(UInt8[]))
        checkfreshdefaults(
            Avro.decode(nestedwriter, UInt8[]; reader_schema=ownedreader),
            Avro.decode(nestedwriter, UInt8[]; reader_schema=ownedreader))
        OwnedDefaults = @NamedTuple{
            scalar::String, nested::Vector{String}, map::Dict{String,String}}
        ownedtyped = Avro.DatumReader(nestedwriter, OwnedDefaults;
                                      reader_schema=ownedreader)
        checkfreshdefaults(ownedtyped(UInt8[]), ownedtyped(UInt8[]))
        ownedcontainer = take!(Avro.tobuffer([NamedTuple(), NamedTuple()];
                                             schema=nestedwriter, codec=:null))
        Avro.Rows(ownedcontainer; reader_schema=ownedreader) do rows
            values = collect(rows)
            @test length(values) == 2
            checkfreshdefaults(values[1], values[2])
        end

        largedefault = repeat("x", 100_000)
        largereader = P("{\"type\":\"record\",\"name\":\"W\",\"fields\":[{\"name\":\"s\",\"type\":\"string\",\"default\":\"" *
                        largedefault * "\"}]}")
        @test Avro.DatumReader(nestedwriter; reader_schema=largereader,
                               limits=Avro.Limits(max_bytes=1024))(UInt8[]).s == largedefault   # max_bytes bounds buffers, not values
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
        checkcompound(Avro.decode(nestedwriter, UInt8[]; reader_schema=compoundreader))
        checkcompound(Avro.DatumReader(nestedwriter; reader_schema=compoundreader)(UInt8[]))
        checkcompound(Avro.decode(nestedwriter, UInt8[], compoundtarget; reader_schema=compoundreader))
        checkcompound(Avro.DatumReader(nestedwriter, compoundtarget; reader_schema=compoundreader)(UInt8[]))
        compoundio = IOBuffer()
        compoundwriter = Avro.Writer(compoundio, nestedwriter)
        for _ in 1:4
            push!(compoundwriter, NamedTuple())
            flush(compoundwriter)
        end
        close(compoundwriter)
        compoundcontainer = take!(compoundio)
        Avro.Rows(compoundcontainer; reader_schema=compoundreader) do rows
            values = collect(rows)
            @test length(values) == 4
            foreach(checkcompound, values)
        end
        Avro.Rows(compoundcontainer; reader_schema=compoundreader,
                  select=(:array_default,)) do rows
            values = collect(rows)
            @test length(values) == 4
            @test all(value -> value.array_default ==
                      Union{Missing,Int32}[Int32(7)], values)
        end
        compoundtable = Avro.Table(compoundcontainer;
                                   reader_schema=compoundreader, ntasks=1)
        @test length(compoundtable) == 4
        checkcompound(first(Tables.rows(compoundtable)))
        if Threads.nthreads() > 1
            paralleltable = Avro.Table(compoundcontainer;
                                       reader_schema=compoundreader, ntasks=2)
            @test length(paralleltable) == 4
            checkcompound(first(Tables.rows(paralleltable)))
        end
        writer = P("""{"type":"record","name":"W","fields":[
            {"name":"payload","type":"bytes"}]}""")
        defaults = join(fill("null", 1000), ",")
        reader = P("""{"type":"record","name":"W","fields":[
            {"name":"added","type":{"type":"array","items":"null"},
             "default":[$defaults]}]}""")
        datumlimits = Avro.Limits(max_values_per_byte=16, value_allowance=100)
        containerlimits = Avro.Limits(max_values_per_byte=16, value_allowance=10)
        datum = Avro.encode(writer, (payload=UInt8[],))
        # the defaults come from the caller's schema, so a thousand of them per row fit tight limits
        @test length(Avro.decode(writer, datum; reader_schema=reader, limits=datumlimits).added) == 1000
        @test length(Avro.DatumReader(writer; reader_schema=reader, limits=datumlimits)(datum).added) == 1000
        io = IOBuffer()
        containerwriter = Avro.Writer(io, writer)
        push!(containerwriter, (payload=UInt8[],))
        close(containerwriter)
        container = take!(io)
        @test length(first(Avro.Rows(container; reader_schema=reader, limits=containerlimits)).added) == 1000
        @test length(Avro.Table(container; reader_schema=reader, limits=containerlimits).added[1]) == 1000
        typed = NamedTuple{(:added,),Tuple{Vector{Missing}}}
        @test length(first(Avro.Rows(container, typed; reader_schema=reader, limits=containerlimits)).added) == 1000
        # many added fields over small rows, under the default limits
        small = P("""{"type":"record","name":"S","fields":[{"name":"a","type":"boolean"}]}""")
        added = join(("{\"name\":\"f$i\",\"type\":\"long\",\"default\":$i}" for i in 1:40), ",")
        grown = P("""{"type":"record","name":"S","fields":[{"name":"a","type":"boolean"},$added]}""")
        smallrows = take!(Avro.tobuffer([(a=true,) for _ in 1:200_000]; schema=small))
        @test Avro.Table(smallrows; reader_schema=grown).f40 == fill(40, 200_000)

        hugewriter = P("""{"type":"array","items":{"type":"record",
            "name":"HugeDefault","fields":[]}}""")
        hugereader = P("""{"type":"array","items":{"type":"record",
            "name":"HugeDefault","fields":[
            {"name":"d","type":"null","default":null}]}}""")
        hugebytes = vcat(Avro.encode(Avro.LongSchema(), typemax(Int64) - 1),
                         UInt8[0])
        hugeerr = try
            Avro.decode(hugewriter, hugebytes; reader_schema=hugereader)
            nothing
        catch caught
            caught
        end
        @test hugeerr isa Avro.LimitError
        @test hugeerr.limit === :max_values_per_byte          # empty records with defaults stop at the cap
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
