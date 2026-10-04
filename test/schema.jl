@testset "Schema model" begin
    P = Avro.parseschema
    weather = P("""{"type":"record","name":"test.Weather","fields":[{"name":"station","type":"string"},{"name":"time","type":"long"},{"name":"temp","type":"int","default":3}]}""")

    @testset "Apache schema-tests.txt (canonical form + CRC-64-AVRO)" begin
        lines = readlines(joinpath(FIXTURES, "apache", "schema-tests.txt"))
        input = canon = fp = nothing
        cases = 0
        function flush()
            input === nothing && return
            s = P(input)
            @test Avro.canonical(s) == canon
            if fp !== nothing
                @test Avro.fingerprint(s) == reinterpret(UInt64, parse(Int64, fp))
            end
            # the canonical form re-parses to the same canonical form
            @test Avro.canonical(P(canon)) == canon
            cases += 1
            input = canon = fp = nothing
            return nothing
        end
        section = :none
        for line in lines
            if startswith(line, "<<INPUT")
                flush()
                input = strip(line[8:end])
                section = :input
            elseif startswith(line, "<<canonical")
                canon = strip(line[12:end])
                section = :canonical
            elseif startswith(line, "<<fingerprint")
                fp = strip(line[14:end])
                section = :none
            elseif startswith(line, "//") || startswith(line, "#")
                section = :none
            elseif strip(line) == "INPUT"
                section = :none
            elseif !isempty(strip(line))
                section === :input && (input = string(input, "\n", line))
                section === :canonical && (canon = string(canon, strip(line)))
            end
        end
        flush()
        @test cases == 34
    end

    @testset "generated corpus: canonical forms and fingerprints equal Java's" begin
        gen = joinpath(FIXTURES, "generated")
        function schemafile(name)
            n = replace(name, r"\.avsc$" => "", r"\.json$" => "")
            if startswith(n, "schemas_")
                g = joinpath(gen, "schemas", replace(n, "schemas_" => "") * ".avsc")
                return isfile(g) ? g : joinpath(FIXTURES, "apache",
                                                replace(n, "schemas_" => "") * ".avsc")
            elseif startswith(n, "evolution_")
                return joinpath(gen, "evolution", replace(n, "evolution_" => "") * ".avsc")
            elseif n == "messageV1_test_schema"
                return joinpath(FIXTURES, "apache", "messageV1", "test_schema.avsc")
            elseif n in ("simple_schema", "withUnion_schema")
                return joinpath(FIXTURES, "apache", "schemas",
                                replace(n, "_schema" => ""), "schema.json")
            end
            return joinpath(FIXTURES, "apache", n * ".avsc")
        end
        checked = 0
        for f in readdir(joinpath(gen, "canonical"))
            endswith(f, ".canonical") || continue
            path = schemafile(replace(f, ".canonical" => ""))
            isfile(path) || continue
            s = P(read(path, String))
            @test Avro.canonical(s) == strip(read(joinpath(gen, "canonical", f), String))
            checked += 1
        end
        @test checked >= 15
        for line in eachline(joinpath(gen, "fingerprints.tsv"))
            name, alg, rest = split(line, '\t')
            hex = split(rest)[1]
            path = schemafile(name)
            isfile(path) || continue
            s = P(read(path, String))
            if alg == "CRC-64-AVRO"
                # avro-tools prints the fingerprint bytes little-endian (as single-object encoding stores them)
                @test bytes2hex(reinterpret(UInt8, [htol(Avro.fingerprint(s))])) == lowercase(hex)
            elseif alg == "MD5"
                @test bytes2hex(Avro.fingerprint(s; algorithm=:md5)) == lowercase(hex)
            else
                @test bytes2hex(Avro.fingerprint(s; algorithm=:sha256)) == lowercase(hex)
            end
        end
        @test_throws ArgumentError Avro.fingerprint(weather; algorithm=:sha1)

        @testset "fingerprint workspace is reserved before hashing" begin
            for algorithm in (:md5, :sha256)
                ample = Avro.Budget(Avro.Limits(); available=1 << 40)
                digest = Avro.hashfingerprint(UInt8[0x01], algorithm, ample)
                @test ample.reserved == Base.summarysize(digest)
                peak = ample.peak
                Avro.close!(ample)

                tight = Avro.Budget(Avro.Limits(); available=1 << 40)
                err = try
                    Avro.reserve!(tight, tight.ceiling - peak + 1)
                    Avro.hashfingerprint(UInt8[0x01], algorithm, tight)
                    nothing
                catch e
                    e
                finally
                    Avro.close!(tight)
                end
                @test err isa Avro.LimitError
                @test err.limit === :max_total_bytes
                @test err.observed == tight.ceiling + 1
                @test err.value == tight.ceiling
            end
        end
    end

    @testset "printing round trips every fixture schema structurally" begin
        negativeschemas = joinpath(FIXTURES, "interop", "negative", "schema")
        function isnegativeschema(path)
            return first(splitpath(relpath(path, negativeschemas))) != ".."
        end
        files = String[]
        for (root, _, fs) in walkdir(FIXTURES), f in fs
            isnegativeschema(root) && continue
            (endswith(f, ".avsc") || f == "schema.json") && push!(files, joinpath(root, f))
        end
        @test any(f -> endswith(f, ".avsc"), readdir(negativeschemas))
        @test all(!isnegativeschema(f) for f in files)
        @test length(files) >= 20
        for f in files
            s = P(read(f, String))
            t = P(Avro.json(s))
            @test t == s
            @test hash(t) == hash(s)
            @test Avro.canonical(t) == Avro.canonical(s)
            @test P(Avro.json(s; pretty=true)) == s
        end
    end

    @testset "fullname algorithm and references" begin
        s = P("""{"type":"record","name":"a.R","namespace":"ignored","fields":[{"name":"x","type":{"type":"enum","name":"E","symbols":["A"]}},{"name":"y","type":"E"},{"name":"z","type":"a.E"}]}""")
        @test Avro.fullname(s) == "a.R"
        @test Avro.fullname(s.fields[1].schema) == "a.E"
        @test s.fields[2].schema === s.fields[1].schema && s.fields[3].schema === s.fields[1].schema
        @test occursin("\"namespace\":\"a\"", Avro.json(s))
        @test !occursin("\"namespace\":\"a\",\"symbols\"", Avro.json(s))   # nested E inherits the namespace
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":[{"name":"x","type":"Missing"}]}""")
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":[{"name":"x","type":"R2"}]}""")
        rec = P("""{"type":"record","name":"LongList","fields":[{"name":"value","type":"long"},{"name":"next","type":["null","LongList"]}]}""")
        @test rec.fields[2].schema.branches[2] === rec
        @test rec == P(Avro.json(rec))
        equalbudget = Avro.Budget(Avro.Limits(); available=1 << 40)
        @test Avro.budgetedschemaequal(rec, P(Avro.json(rec)), equalbudget)
        @test equalbudget.pending == equalbudget.reserved == 0              # equality memo is operation scratch
        Avro.close!(equalbudget)
        @test Avro.canonical(rec) == "{\"name\":\"LongList\",\"type\":\"record\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"},{\"name\":\"next\",\"type\":[\"null\",\"LongList\"]}]}"
        # a bare reference may name a null-namespace type from inside a namespace
        s2 = P("""{"type":"record","name":"R","fields":[{"name":"e","type":{"type":"fixed","name":"F","size":1}},{"name":"n","type":{"type":"record","name":"ns.Inner","fields":[{"name":"f","type":"F"}]}}]}""")
        @test s2.fields[2].schema.fields[1].schema === s2.fields[1].schema
        # non-string namespace is rejected (deliberate; Java accepts)
        @test_throws Avro.SchemaError P("""{"type":"record","name":"a.R","namespace":5,"fields":[]}""")
        # ignored namespace text is not validated otherwise
        @test Avro.fullname(P("""{"type":"record","name":"a.R","namespace":"not valid!","fields":[]}""")) == "a.R"
    end

    @testset "schema-object grammar and contextual attributes" begin
        @test_throws Avro.SchemaError P("""{"type":{"type":"string"}}""")
        @test_throws Avro.SchemaError P("""{"type":["null","int"]}""")
        @test_throws Avro.SchemaError P("""{"type":"union"}""")
        @test_throws Avro.SchemaError P("""{"type":"E"}""")            # a named reference must be a string
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["A"]}},{"name":"f","type":{"type":"E"}}]}""")
        @test_throws Avro.SchemaError P("""{"type":"bogus"}""")
        @test_throws Avro.SchemaError P("""5""")
        @test_throws Avro.SchemaError P("""{"name":"R"}""")
        # undefined attributes are metadata in every context; defined ones are grammar only where defined
        s = P("""{"type":"int","name":123}""")
        @test s isa Avro.IntSchema && s.props["name"] == 123
        @test Avro.json(s) == "{\"type\":\"int\",\"name\":123}"
        a = P("""{"type":"array","items":"int","size":"x"}""")
        @test a.props["size"] == "x"
        fx = P("""{"type":"fixed","name":"F","size":4,"doc":7}""")        # fixed defines no doc
        @test fx.props["doc"] == 7
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","doc":7,"fields":[]}""")
        @test_throws Avro.SchemaError P("""{"type":"fixed","name":"F","size":"4"}""")
        @test_throws Avro.SchemaError P("""{"type":"fixed","name":"F","size":-1}""")
        @test_throws Avro.SchemaError P("""{"type":"fixed","name":"F"}""")
        @test_throws Avro.SchemaError P("""{"type":"enum","name":"E","symbols":[1]}""")
        @test_throws Avro.SchemaError P("""{"type":"enum","name":"E","symbols":["a","a"]}""")
        @test_throws Avro.SchemaError P("""{"type":"enum","name":"E","symbols":["9"]}""")
        @test_throws Avro.SchemaError P("""{"type":"enum","name":"E","symbols":["A"],"default":"B"}""")
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":[{"name":"a","type":"int","order":"up"}]}""")
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"a","type":"int"}]}""")
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":{}}""")
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":[1]}""")
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":[{"type":"int"}]}""")
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":[{"name":"a"}]}""")
        @test_throws Avro.SchemaError P("""{"type":"array"}""")
        @test_throws Avro.SchemaError P("""{"type":"map"}""")
        nestedpatherror = try
            P("""{"type":"array","items":{"type":"record","name":"PathR","fields":[{"name":"x","type":7}]}}""")
            nothing
        catch err
            err
        end
        @test nestedpatherror isa Avro.SchemaError
        @test nestedpatherror.path == "\$.items.fields[0].type"
        @test P("""{"type":"enum","name":"E","symbols":[]}""") isa Avro.EnumSchema     # empty enum is valid
        @test P("[]") isa Avro.UnionSchema                                            # empty union is valid
        @test Avro.canonical(P("[]")) == "[]"
        e = P("""{"type":"enum","name":"E","symbols":["A","B"],"default":"B"}""")
        @test e.default.index == 2 && e.symbolindex["B"] == 2
        err = P("""{"type":"error","name":"Err","fields":[{"name":"m","type":"string"}]}""")
        @test err.iserror && Avro.kind(err) == :error && occursin("\"type\":\"error\"", Avro.json(err)) && Avro.canonical(err) == Avro.canonical(P(replace(Avro.json(err), "error" => "record")))
        @test err != P(replace(Avro.json(err), "error" => "record"))
    end

    @testset "names, reserved names, aliases" begin
        # an invalid name ending in a multi-byte character is reported with the escaped text (fuzz finding: byte-indexed slicing)
        e = try; Avro.parseschema("{\"type\":\"enum\",\"name\":\"r\",\"symbols\":[\"\u0380\",\"\"]}"); nothing; catch err; err; end
        @test e isa Avro.SchemaError && occursin("invalid enum symbol \"\u0380\"", sprint(showerror, e))
        @test Avro.escapename("a\u0380") == "a\u0380" && Avro.escapename("") == "" && Avro.escapename("x\"y") == "x\\\"y" && Avro.escapename("t\tb") == "t\\tb"
        hugediagnosticname = "!"^1_000_000
        Avro.escapename(hugediagnosticname)
        GC.gc()
        @test sizeof(Avro.escapename(hugediagnosticname)) < 256
        @test @allocated(Avro.escapename(hugediagnosticname)) < 128 * 1024
        @test_throws Avro.SchemaError P("""{"type":"record","name":"9R","fields":[]}""")
        @test_throws Avro.SchemaError P("""{"type":"record","name":"a-b","fields":[]}""")
        @test_throws Avro.SchemaError P("""{"type":"record","name":"int","fields":[]}""")            # reserved in the null namespace
        @test Avro.fullname(P("""{"type":"record","name":"a.int","fields":[]}""")) == "a.int"       # valid with a namespace
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"fixed","name":"R","size":1}}]}""")
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":[{"name":"bad name","type":"int"}]}""")
        r = P("""{"type":"record","name":"R","namespace":"a","aliases":["R","Bar","a.Bar","x.Y"],"fields":[{"name":"f","type":"int","aliases":["f","g"]}]}""")
        @test r.aliases == ["a.Bar", "x.Y"]                       # self-alias ignored, relative alias normalised, duplicates folded
        @test r.rawaliases == ["R", "Bar", "a.Bar", "x.Y"]
        @test r.fields[1].aliases == ["g"]
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":[{"name":"f","type":"int","aliases":["g"]},{"name":"g","type":"int"}]}""")
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"fixed","name":"F","size":1,"aliases":["G"]}},{"name":"b","type":{"type":"fixed","name":"G","size":1}}]}""")
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","aliases":[1],"fields":[]}""")
        arbitrary = P("""{"type":"record","name":"R","aliases":["bad-alias"],"fields":[{"name":"f","type":"int","aliases":["bad-field"]}]}""")
        @test arbitrary.aliases == ["bad-alias"]
        @test arbitrary.fields[1].aliases == ["bad-field"]
        @test !Avro.graphinfo(arbitrary).repaired_names
        constructed = Avro.RecordSchema("R"; aliases=["bad-alias"], fields=[Avro.Field("f", Avro.IntSchema(); aliases=["bad-field"])])
        @test constructed == arbitrary
        rep = P("""{"type":"record","name":"R","aliases":["bad-alias"],"fields":[{"name":"bad-field","type":"int"}]}"""; allow_invalid_names=true)
        @test Avro.graphinfo(rep).repaired_names
        @test_throws ArgumentError Avro.canonical(rep)
        @test_throws ArgumentError Avro.fingerprint(rep)
        @test Avro.json(rep) == """{"type":"record","name":"R","aliases":["bad-alias"],"fields":[{"name":"bad-field","type":"int"}]}"""
    end

    @testset "defaults: the recursive rule" begin
        @test P("""{"type":"record","name":"R","fields":[{"name":"a","type":"int","default":1}]}""").fields[1].default.valid
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":[{"name":"a","type":"int","default":"x"}]}""")
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":[{"name":"a","type":"int","default":2147483648}]}""")
        @test P("""{"type":"record","name":"R","fields":[{"name":"a","type":"long","default":9223372036854775807}]}""").fields[1].default.valid
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":[{"name":"a","type":"long","default":9223372036854775808}]}""")
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":[{"name":"a","type":"int","default":1.0}]}""")
        @test P("""{"type":"record","name":"R","fields":[{"name":"a","type":"double","default":1}]}""").fields[1].default.valid
        @test P("""{"type":"record","name":"R","fields":[{"name":"a","type":"float","default":"NaN"}]}""").fields[1].default.valid   # Java-compatible extension
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":[{"name":"a","type":"float","default":"nan"}]}""")
        u = P("""{"type":"record","name":"R","fields":[{"name":"a","type":["null","int"],"default":null},{"name":"b","type":["int","null"],"default":5}]}""")
        @test u.fields[1].default.branch == 1 && u.fields[2].default.branch == 1
        @test P("""{"type":"record","name":"R","fields":[{"name":"a","type":["null","int"],"default":5}]}""").fields[1].default.branch == 2   # any branch in declaration order
        @test P("""{"type":"record","name":"R","fields":[{"name":"a","type":["int","null"],"default":null}]}""").fields[1].default.branch == 2
        @test P("""{"type":"record","name":"R","fields":[{"name":"b","type":"bytes","default":"\\u00ff\\u0000"}]}""").fields[1].default.valid
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":[{"name":"b","type":"bytes","default":"\\u0100"}]}""")
        @test P("""{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"fixed","name":"F","size":2},"default":"ab"}]}""").fields[1].default.valid
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"fixed","name":"F","size":2},"default":"abc"}]}""")
        @test P("""{"type":"record","name":"R","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["A"]},"default":"A"}]}""").fields[1].default.valid
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["A"]},"default":"B"}]}""")
        @test P("""{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"array","items":"int"},"default":[1,2]}]}""").fields[1].default.valid
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"array","items":"int"},"default":[1,"x"]}]}""")
        @test P("""{"type":"record","name":"R","fields":[{"name":"m","type":{"type":"map","values":"string"},"default":{"k":"v"}}]}""").fields[1].default.valid
        # nested record defaults: a missing nested field is supplied from its own default; unknown members are ignored
        nested = """{"type":"record","name":"Outer","fields":[{"name":"in","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int","default":1}]},"default":{}}]}"""
        @test P(nested).fields[1].default.valid
        @test P(replace(nested, "\"default\":{}" => "\"default\":{\"x\":2,\"extra\":true}")).fields[1].default.valid
        @test_throws Avro.SchemaError P("""{"type":"record","name":"Outer","fields":[{"name":"in","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]},"default":{}}]}""")
        # Revalidate every default after all recursive record shells are complete. An omitted
        # required recursive field has no finite default, regardless of field order.
        recursive = """{"type":"record","name":"RecursiveDefault","fields":[
            {"name":"next","type":"RecursiveDefault","default":{}}]}"""
        @test_throws Avro.SchemaError P(recursive)
        @test_throws Avro.SchemaError P("""{"type":"record","name":"LateRequired","fields":[
            {"name":"next","type":"LateRequired","default":{}},
            {"name":"x","type":"int"}]}""")
        @test_throws Avro.SchemaError P("""{"type":"record","name":"MutualA","fields":[
            {"name":"b","type":{"type":"record","name":"MutualB","fields":[
                {"name":"a","type":"MutualA","default":{}}]},"default":{}}]}""")
        @test_throws ArgumentError Avro.RecordSchema(
            ref -> [Avro.Field("next", ref; default=(;))], "PublicRecursiveDefault")
        repairedrecursive = P(recursive; allow_invalid_defaults=true)
        @test !repairedrecursive.fields[1].default.valid
        @test repairedrecursive.fields[1].default.branch == 0
        @test Avro.graphinfo(repairedrecursive).repaired_defaults

        finite = P("""{"type":"record","name":"FiniteRoot","fields":[
            {"name":"node","type":{"type":"record","name":"FiniteNode","fields":[
                {"name":"next","type":["null","FiniteNode"],"default":null}]},
             "default":{}}]}""")
        @test finite.fields[1].default.valid
        @test finite.fields[1].schema.fields[1].default.branch == 1

        # A pending record can be an early false-positive union branch. The complete graph must
        # select the first branch that accepts the default after all fields are installed.
        switched = P("""{"type":"record","name":"DefaultSwitch","fields":[
            {"name":"u","type":["DefaultSwitch",{"type":"map","values":"int"}],
             "default":{}},
            {"name":"x","type":"int"}]}""")
        @test switched.fields[1].default.valid
        @test switched.fields[1].default.branch == 2

        # Logical defaults must be valid in the logical domain, not only in the base Avro kind.
        @test P("""{"type":"record","name":"TimeMillisBoundary","fields":[
            {"name":"x","type":{"type":"int","logicalType":"time-millis"},
             "default":86399999}]}""").fields[1].default.valid
        @test_throws Avro.SchemaError P("""{"type":"record","name":"BadTimeMillis","fields":[
            {"name":"x","type":{"type":"int","logicalType":"time-millis"},
             "default":86400000}]}""")
        @test_throws Avro.SchemaError P("""{"type":"record","name":"NegativeTimeMillis","fields":[
            {"name":"x","type":{"type":"int","logicalType":"time-millis"},
             "default":-1}]}""")
        @test P("""{"type":"record","name":"TimeMicrosBoundary","fields":[
            {"name":"x","type":{"type":"long","logicalType":"time-micros"},
             "default":86399999999}]}""").fields[1].default.valid
        @test_throws Avro.SchemaError P("""{"type":"record","name":"BadTimeMicros","fields":[
            {"name":"x","type":{"type":"long","logicalType":"time-micros"},
             "default":86400000000}]}""")
        @test P("""{"type":"record","name":"UUIDBoundary","fields":[
            {"name":"x","type":{"type":"string","logicalType":"uuid"},
             "default":"00000000-0000-0000-0000-000000000000"}]}""").fields[1].default.valid
        @test_throws Avro.SchemaError P("""{"type":"record","name":"BadUUID","fields":[
            {"name":"x","type":{"type":"string","logicalType":"uuid"},
             "default":"not-a-uuid"}]}""")

        decimaltype = "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":2}"
        @test P("{\"type\":\"record\",\"name\":\"DecimalBoundary\",\"fields\":[{\"name\":\"x\",\"type\":" *
                decimaltype * ",\"default\":\"c\"}]}").fields[1].default.valid
        @test P("{\"type\":\"record\",\"name\":\"NegativeDecimalBoundary\",\"fields\":[{\"name\":\"x\",\"type\":" *
                decimaltype * ",\"default\":\"\\u009d\"}]}").fields[1].default.valid
        @test_throws Avro.SchemaError P("{\"type\":\"record\",\"name\":\"EmptyDecimal\",\"fields\":[{\"name\":\"x\",\"type\":" *
                                           decimaltype * ",\"default\":\"\"}]}")
        @test_throws Avro.SchemaError P("{\"type\":\"record\",\"name\":\"WideDecimalValue\",\"fields\":[{\"name\":\"x\",\"type\":" *
                                           decimaltype * ",\"default\":\"d\"}]}")
        @test_throws Avro.SchemaError P("""{"type":"record","name":"FixedDecimalValue","fields":[
            {"name":"x","type":{"type":"fixed","name":"DecimalFixed","size":1,
             "logicalType":"decimal","precision":2},"default":"d"}]}""")
        widevalid = repeat("\\u0000", 16) * "\\u0001"
        wideinvalid = "\\u0001" * repeat("\\u0000", 16)
        wideprecision = "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38}"
        @test P("{\"type\":\"record\",\"name\":\"WideValidDecimal\",\"fields\":[{\"name\":\"x\",\"type\":" *
                wideprecision * ",\"default\":\"" * widevalid * "\"}]}").fields[1].default.valid
        @test_throws Avro.SchemaError P("{\"type\":\"record\",\"name\":\"WideInvalidDecimal\",\"fields\":[{\"name\":\"x\",\"type\":" *
                                           wideprecision * ",\"default\":\"" * wideinvalid * "\"}]}")
        repairedlogical = P("""{"type":"record","name":"RepairedLogical","fields":[
            {"name":"x","type":{"type":"string","logicalType":"uuid"},
             "default":"bad"}]}"""; allow_invalid_defaults=true)
        @test !repairedlogical.fields[1].default.valid
        @test Avro.graphinfo(repairedlogical).repaired_defaults
        @test_throws ArgumentError Avro.Field(
            "x", Avro.IntSchema(; logical=Avro.TimeMillis()); default=Int64(86_400_000))
        @test_throws ArgumentError Avro.Field(
            "x", Avro.StringSchema(; logical=Avro.UUIDLogical()); default="bad")
        @test_throws ArgumentError Avro.Field(
            "x", Avro.BytesSchema(; logical=Avro.DecimalLogical(2)); default="d")
        @test_throws Avro.SchemaError P("""{"type":"record","name":"R","fields":[{"name":"a","type":"null","default":1}]}""")
        @test P("""{"type":"record","name":"R","fields":[{"name":"a","type":"null","default":null}]}""").fields[1].default.valid
        # invalid defaults are kept verbatim under allow_invalid_defaults
        bad = P("""{"type":"record","name":"R","fields":[{"name":"a","type":"int","default": "x" }]}"""; allow_invalid_defaults=true)
        @test !bad.fields[1].default.valid && Avro.graphinfo(bad).repaired_defaults
        @test occursin("\"default\":\"x\"", Avro.json(bad))
        @test Avro.canonical(bad) == Avro.canonical(P("""{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}"""))
        # source text of defaults is re-emitted verbatim
        v = P("""{"type":"record","name":"R","fields":[{"name":"d","type":"double","default":1.50}]}""")
        @test occursin("\"default\":1.50", Avro.json(v))
        # equality compares decoded defaults with isequal semantics
        @test P("""{"type":"record","name":"R","fields":[{"name":"d","type":"double","default":1.0}]}""") != P("""{"type":"record","name":"R","fields":[{"name":"d","type":"double","default":1.00}]}""") ||
              P("""{"type":"record","name":"R","fields":[{"name":"d","type":"double","default":1.0}]}""").fields[1].default.json isa Avro.JSONNumber
    end

    @testset "logical types are evaluated in context" begin
        d = P("""{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}""")
        @test d.logical == Avro.DecimalLogical(10, 2)
        @test P("""{"type":"bytes","logicalType":"decimal","precision":10}""").logical == Avro.DecimalLogical(10, 0)
        @test P("""{"type":"bytes","logicalType":"decimal","precision":0}""").logical === nothing
        @test P("""{"type":"bytes","logicalType":"decimal","precision":3,"scale":4}""").logical === nothing
        @test P("""{"type":"bytes","logicalType":"decimal","precision":"x"}""").logical === nothing
        @test P("""{"type":"int","logicalType":"decimal","precision":3}""").logical === nothing
        @test P("""{"type":"fixed","name":"F","size":4,"logicalType":"decimal","precision":9}""").logical == Avro.DecimalLogical(9, 0)
        @test P("""{"type":"fixed","name":"F","size":4,"logicalType":"decimal","precision":10}""").logical === nothing
        @test P("""{"type":"fixed","name":"F","size":0,"logicalType":"decimal","precision":1}""").logical === nothing     # fixed(0): no DomainError
        @test P("""{"type":"fixed","name":"F","size":16,"logicalType":"decimal","precision":38}""").logical == Avro.DecimalLogical(38, 0)
        @test Avro.maxdecimalprecision(16) == 38 && Avro.maxdecimalprecision(1) == 2 && Avro.maxdecimalprecision(0) == 0
        @test Avro.maxdecimalprecision(4721) == 11368
        @test Avro.maxdecimalprecision(typemax(Int)) == 2776511644261678565
        @test P("""{"type":"fixed","name":"F","size":4721,"logicalType":"decimal","precision":11368}""").logical == Avro.DecimalLogical(11368, 0)
        @test P("""{"type":"fixed","name":"F","size":4721,"logicalType":"decimal","precision":11369}""").logical === nothing
        @test P("""{"type":"string","logicalType":"uuid"}""").logical isa Avro.UUIDLogical
        @test P("""{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}""").logical isa Avro.UUIDLogical
        @test P("""{"type":"fixed","name":"U","size":15,"logicalType":"uuid"}""").logical === nothing
        @test P("""{"type":"int","logicalType":"date"}""").logical isa Avro.DateLogical
        @test P("""{"type":"long","logicalType":"date"}""").logical === nothing
        @test P("""{"type":"int","logicalType":"time-millis"}""").logical isa Avro.TimeMillis
        @test P("""{"type":"long","logicalType":"time-micros"}""").logical isa Avro.TimeMicros
        @test P("""{"type":"long","logicalType":"timestamp-nanos"}""").logical isa Avro.TimestampNanos
        @test P("""{"type":"long","logicalType":"local-timestamp-micros"}""").logical isa Avro.LocalTimestampMicros
        @test P("""{"type":"fixed","name":"D","size":12,"logicalType":"duration"}""").logical isa Avro.DurationLogical
        @test P("""{"type":"fixed","name":"D","size":11,"logicalType":"duration"}""").logical === nothing
        @test P("""{"type":"bytes","logicalType":"big-decimal"}""").logical == Avro.UnknownLogical("big-decimal")
        @test P("""{"type":"int","logicalType":5}""").logical === nothing
        # the raw attributes survive in props and re-serialise
        s = P("""{"type":"int","logicalType":"decimal","precision":3}""")
        @test s.props["logicalType"] == "decimal" && Avro.json(s) == "{\"type\":\"int\",\"logicalType\":\"decimal\",\"precision\":3}"
        # canonical form strips the annotation
        @test Avro.canonical(d) == "\"bytes\""
        @test d != P("""{"type":"bytes","logicalType":"decimal","precision":10,"scale":3}""")
    end

    @testset "unions" begin
        @test_throws Avro.SchemaError P("""["int","int"]""")
        @test_throws Avro.SchemaError P("""[["int"]]""")
        @test_throws Avro.SchemaError P("""[{"type":"array","items":"int"},{"type":"array","items":"string"}]""")
        @test P("""[{"type":"fixed","name":"A","size":1},{"type":"fixed","name":"B","size":1}]""") isa Avro.UnionSchema
        @test_throws Avro.SchemaError P("""[{"type":"fixed","name":"A","size":1},"A"]""")
        @test P("""["null",{"type":"map","values":"int"},{"type":"record","name":"map","fields":[]}]""") isa Avro.UnionSchema  # label collision is a JSON-time concern
    end

    @testset "limits" begin
        @test_throws Avro.LimitError P("""{"type":"record","name":"R","fields":[]}"""; limits=Avro.Limits(max_schema_bytes=10))
        @test_throws Avro.LimitError P("""{"type":"array","items":{"type":"array","items":{"type":"array","items":"int"}}}"""; limits=Avro.Limits(max_schema_depth=2))
        @test_throws Avro.LimitError P("""{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"int"}]}"""; limits=Avro.Limits(max_fields=1))
        @test_throws Avro.LimitError P("""["null","int","string"]"""; limits=Avro.Limits(max_union_branches=2))
        @test_throws Avro.LimitError P("""{"type":"enum","name":"E","symbols":["A","B"]}"""; limits=Avro.Limits(max_enum_symbols=1))
        @test_throws Avro.LimitError P("""{"type":"record","name":"Rrrrrrrrrr","fields":[]}"""; limits=Avro.Limits(max_name_bytes=5))
        @test_throws Avro.LimitError P("""{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"fixed","name":"F","size":1}}]}"""; limits=Avro.Limits(max_named_types=1))
        @test_throws Avro.LimitError P("""{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"int"},{"name":"c","type":"int"}]}"""; limits=Avro.Limits(max_schema_nodes=3))
        # IO sources are read incrementally under the limit
        @test P(IOBuffer("\"int\"")) isa Avro.IntSchema
        @test_throws Avro.LimitError P(IOBuffer("\"string\""); limits=Avro.Limits(max_schema_bytes=5))
        @test P(Vector{UInt8}(codeunits("\"long\""))) isa Avro.LongSchema
        @test P(view(Vector{UInt8}(codeunits("xx\"long\"")), 3:8)) isa Avro.LongSchema
        @test P(SubString("\"float\" ", 1, 7)) isa Avro.FloatSchema
    end

    @testset "immutability, identity, equality and hashing on cycles" begin
        s = P("""{"type":"record","name":"N","fields":[{"name":"next","type":["null","N"]},{"name":"p","type":"int","k":[1,{"a":2}]}]}""")
        @test_throws Avro.FrozenError push!(s.fields, s.fields[1])
        @test_throws Avro.FrozenError push!(s.fields[2].props["k"].items, 3)
        @test_throws Avro.FrozenError (s.fields[2].props["k"][2].members["b"] = 1)
        @test_throws Avro.FrozenError push!(s.fields[1].schema.branches, s)
        t = P(Avro.json(s))
        @test s == t && hash(s) == hash(t) && s !== t
        @test Avro.nodeid(s) == 0 && Avro.graphinfo(s).nodes == 4 && Avro.graphinfo(s).namedtypes == 1
        @test s != P("""{"type":"record","name":"N","fields":[{"name":"next","type":["null","N"]},{"name":"p","type":"int","k":[1,{"a":3}]}]}""")
        @test s != P("""{"type":"record","name":"N","fields":[{"name":"next","type":["null","N"]},{"name":"p","type":"int"}]}""")
        @test weather != s && hash(weather) != hash(s)
        # props are unordered maps
        @test P("""{"type":"int","a":1,"b":2}""") == P("""{"type":"int","b":2,"a":1}""")
        @test P("""{"type":"int","a":1}""") != P("""{"type":"int","a":1.0}""")     # raw number tokens differ lexically
        @test Avro.parsingequivalent(P("""{"type":"int","a":1}"""), P("\"int\""))
    end

    @testset "public constructors" begin
        i = Avro.IntSchema(; logical=Avro.DateLogical())
        @test i.logical isa Avro.DateLogical && i.props["logicalType"] == "date" && Avro.json(i) == "{\"type\":\"int\",\"logicalType\":\"date\"}"
        b = Avro.BytesSchema(; logical=Avro.DecimalLogical(9, 2), props=(doc="x",))
        @test b.logical == Avro.DecimalLogical(9, 2) && b.props["precision"] == 9 && b == P(Avro.json(b))
        for invalid in (Avro.IntSchema(; logical=Avro.UUIDLogical()),
                        Avro.BytesSchema(; logical=Avro.DateLogical()),
                        Avro.StringSchema(; logical=Avro.TimestampMillis()))
            @test invalid.logical === nothing
            @test invalid == P(Avro.json(invalid))
        end
        @test_throws ArgumentError Avro.IntSchema(; props=(type="x",))
        @test_throws ArgumentError Avro.IntSchema(; logical=Avro.DateLogical(), props=(logicalType="y",))
        @test Avro.NullSchema() == P("\"null\"") && Avro.StringSchema() == P("\"string\"")
        @test Avro.DoubleSchema(; props=(a=[1, "x"],)).props["a"] == Avro.JSONArray(Avro.FrozenVector{Any}(Any[1, "x"], false))
    end
end

@testset "public complex constructors, recursion, import, minsize" begin
    P = Avro.parseschema
    r = Avro.RecordSchema("Weather"; namespace="test", doc="d", fields=[Avro.Field("station", Avro.StringSchema()), Avro.Field("temp", Avro.IntSchema(); default=3, order=:descending, aliases=["t"], doc="fd")])
    @test r == P(Avro.json(r)) && Avro.fullname(r) == "test.Weather" && r.fields[2].default.branch == 0 && r.fields[2].order == :descending
    @test Avro.json(r) == """{"type":"record","name":"Weather","namespace":"test","doc":"d","fields":[{"name":"station","type":"string"},{"name":"temp","type":"int","doc":"fd","default":3,"order":"descending","aliases":["t"]}]}"""
    @test_throws ArgumentError Avro.RecordSchema("9x")
    @test_throws ArgumentError Avro.RecordSchema("int")
    @test_throws ArgumentError Avro.RecordSchema("R"; fields=[Avro.Field("a", Avro.IntSchema()), Avro.Field("a", Avro.IntSchema())])
    @test_throws ArgumentError Avro.RecordSchema("R"; fields=[Avro.Field("a", Avro.IntSchema(); aliases=["b"]), Avro.Field("b", Avro.IntSchema())])
    @test_throws ArgumentError Avro.RecordSchema("R"; props=(fields=1,))
    @test_throws ArgumentError Avro.Field("a", Avro.IntSchema(); default="x")
    @test_throws ArgumentError Avro.Field("a", Avro.IntSchema(); order=:up)
    @test Avro.Field("a", Avro.UnionSchema([Avro.NullSchema(), Avro.IntSchema()]); default=nothing).default.branch == 1
    @test Avro.Field("a", Avro.UnionSchema([Avro.NullSchema(), Avro.IntSchema()]); default=missing).default.branch == 1
    @test Avro.Field("a", Avro.UnionSchema([Avro.IntSchema(), Avro.NullSchema()]); default=7).default.branch == 1
    @test Avro.Field("a", Avro.ArraySchema(Avro.IntSchema()); default=[1, 2]).default.span == "[1,2]"
    @test Avro.Field("a", Avro.MapSchema(Avro.StringSchema()); default=Dict("k" => "v")).default.valid
    @test Avro.Field("a", Avro.DoubleSchema(); default=1.5).default.json == Avro.JSONNumber("1.5")
    e = Avro.EnumSchema("E", ["A", "B"]; namespace="n", default="B", aliases=["Old"], doc="doc")
    @test e == P(Avro.json(e)) && e.default.index == 2 && e.aliases == ["n.Old"]
    @test_throws ArgumentError Avro.EnumSchema("E", ["A"]; default="Z")
    @test_throws ArgumentError Avro.EnumSchema("E", ["A", "A"])
    @test_throws ArgumentError Avro.EnumSchema("E", ["9"])
    f = Avro.FixedSchema("D", 12; logical=Avro.DurationLogical())
    @test f == P(Avro.json(f)) && f.logical isa Avro.DurationLogical && f.props["logicalType"] == "duration"
    @test Avro.FixedSchema("F", 4; logical=Avro.DecimalLogical(9, 2)).logical == Avro.DecimalLogical(9, 2)
    @test Avro.FixedSchema("F", 0; logical=Avro.DecimalLogical(1)).logical === nothing
    @test_throws ArgumentError Avro.FixedSchema("F", -1)
    u = Avro.UnionSchema([Avro.NullSchema(), Avro.StringSchema()])
    @test u == P("[\"null\",\"string\"]")
    @test_throws ArgumentError Avro.UnionSchema([Avro.IntSchema(), Avro.IntSchema()])
    @test_throws ArgumentError Avro.UnionSchema([Avro.UnionSchema([Avro.IntSchema()])])
    @test_throws ArgumentError Avro.UnionSchema([1])
    a = Avro.ArraySchema(Avro.MapSchema(Avro.LongSchema(); props=(x=1,)))
    @test a == P("""{"type":"array","items":{"type":"map","values":"long","x":1}}""")
    # recursion through the builder form, mutual recursion through nested builders, failure cleanup
    ll = Avro.RecordSchema("LongList") do ref
        [Avro.Field("value", Avro.LongSchema()), Avro.Field("next", Avro.UnionSchema([Avro.NullSchema(), ref]))]
    end
    @test ll == P(Avro.json(ll)) && ll.fields[2].schema.branches[2] === ll && Avro.graphinfo(ll).nodes == 4
    mutual = Avro.RecordSchema("A") do ra
        b = Avro.RecordSchema("B") do rb
            [Avro.Field("a", Avro.UnionSchema([Avro.NullSchema(), ra])), Avro.Field("self", Avro.UnionSchema([Avro.NullSchema(), rb]))]
        end
        [Avro.Field("b", b)]
    end
    @test mutual == P(Avro.json(mutual)) && Avro.graphinfo(mutual).namedtypes == 2
    @test_throws ErrorException Avro.RecordSchema("X") do ref; error("boom"); end
    @test Avro.builderdepth() == 0
    # an already-finalised child is deep-copied with fresh ids; the same child twice is one definition
    pt = Avro.RecordSchema("Point"; fields=[Avro.Field("x", Avro.DoubleSchema())])
    two = Avro.RecordSchema("Seg"; fields=[Avro.Field("a", pt), Avro.Field("b", pt)])
    @test two.fields[1].schema !== pt && two.fields[1].schema === two.fields[2].schema && two == P(Avro.json(two))
    @test Avro.nodeid(two) == 0 && Avro.graphinfo(two).nodes == 3
    left = Avro.RecordSchema("Duplicate"; fields=[Avro.Field("x", Avro.IntSchema())])
    right = Avro.RecordSchema("Duplicate"; fields=[Avro.Field("y", Avro.StringSchema())])
    @test_throws ArgumentError Avro.RecordSchema("ConflictingChildren"; fields=[Avro.Field("left", left), Avro.Field("right", right)])
    @test_throws ArgumentError Avro.RecordSchema("ConflictingBuilder") do _
        nestedleft = Avro.RecordSchema("NestedDuplicate"; fields=[Avro.Field("x", Avro.IntSchema())])
        nestedright = Avro.RecordSchema("NestedDuplicate"; fields=[Avro.Field("y", Avro.StringSchema())])
        [Avro.Field("left", nestedleft), Avro.Field("right", nestedright)]
    end
    # minsize
    @test Avro.minsize(ll) == 2 && Avro.minsize(P("[]")) == typemax(Int) && Avro.minsize(P("""{"type":"enum","name":"E","symbols":[]}""")) == typemax(Int)
    @test Avro.minsize(P("""{"type":"record","name":"R","fields":[{"name":"n","type":"R"}]}""")) == typemax(Int)
    @test Avro.minsize(P("""{"type":"record","name":"R","fields":[{"name":"n","type":{"type":"array","items":"R"}}]}""")) == 1
    @test Avro.minsize(P("""{"type":"record","name":"A","fields":[{"name":"b","type":{"type":"record","name":"B","fields":[{"name":"a","type":"A"}]}}]}""")) == typemax(Int)
    @test Avro.minsize(P("""{"type":"fixed","name":"F","size":7}""")) == 7 && Avro.minsize(P("\"double\"")) == 8 && Avro.minsize(P("\"null\"")) == 0
    @test Avro.minsize(P("""["int","string"]""")) == 2 && Avro.minsize(P("[" * join(["{\"type\":\"fixed\",\"name\":\"F$i\",\"size\":$(i == 64 ? 0 : 10)}" for i in 0:64], ",") * "]")) == 2   # the 65th branch (size 0) needs a 2-byte index
    huge = Avro.RecordSchema("Huge"; fields=[Avro.Field("a", Avro.FixedSchema("A", typemax(Int) - 1)), Avro.Field("b", Avro.FixedSchema("B", 2))])
    @test Avro.minsize(huge) == typemax(Int)
    @test Avro.varintlength(0) == 1 && Avro.varintlength(63) == 1 && Avro.varintlength(64) == 2

    legacy_fixed_json = """{"type":"record","name":"LegacyFixedOrdinals","fields":[
        {"name":"plain","type":"long"},
        {"name":"first","type":{"type":"fixed","size":1}},
        {"name":"nested","type":{"type":"array","items":{"type":"fixed","size":2}}}]}
        """
    @test_throws Avro.SchemaError Avro.parseschema(legacy_fixed_json)
    @test_throws MethodError Avro.parseschema(legacy_fixed_json; legacy_fixed_names=true)

    @testset "construction scope enforces graph limits (plan §4.4, amendment round 1)" begin
        parse_limits = Avro.Limits(max_depth=1, max_schema_depth=7)
        parse_budget = Avro.Budget(parse_limits; available=1 << 40)
        parse_ctx = Avro.ParseContext(parse_limits, parse_budget, false, false)
        expected_parse_state = Avro.vectorbytes(Avro.RecordSchema, parse_limits.max_schema_depth) +
                               Avro.vectorbytes(Avro.NodeMeta, 16) + Avro.frozendictshell() + 128 +
                               Avro.vectorbytes(Avro.ParsePathSegment,
                                                min(parse_limits.max_schema_depth, 16)) +
                               Avro.shellbytes(Avro.ParsePath)
        @test parse_budget.reserved == expected_parse_state
        Avro.close!(parse_budget)

        generated_union = Avro.UnionSchema((Avro.FixedSchema("Generated_$i", 1) for i in 1:5))
        @test generated_union.branches.cap == length(generated_union.branches) == 5
        child = Avro.RecordSchema("Copied"; fields=[Avro.Field("x", Avro.LongSchema())])
        copied = Avro.ArraySchema(child).items
        @test copied isa Avro.RecordSchema
        @test copied.fields.cap == length(copied.fields) == 1

        tight = Avro.Limits(max_schema_nodes=1)
        @test_throws Avro.LimitError Avro.ArraySchema(Avro.LongSchema(); limits=tight)
        @test_throws Avro.LimitError Avro.NullSchema(; limits=Avro.Limits(max_schema_nodes=0))
        @test_throws Avro.LimitError Avro.FixedSchema("x"^2000, 4)                       # name bytes
        @test_throws Avro.LimitError Avro.FixedSchema("F", 4; limits=Avro.Limits(max_named_types=0))
        @test_throws Avro.LimitError Avro.schema(NamedTuple{(:a,),Tuple{Int64}}; limits=Avro.Limits(max_schema_nodes=1))
        @test_throws Avro.LimitError Avro.EnumSchema("E", ["a", "b", "c"]; limits=Avro.Limits(max_enum_symbols=2))
        @test_throws Avro.LimitError Avro.EnumSchema("E", ["ab"]; limits=Avro.Limits(max_name_bytes=1))
        oversizedsymbol = repeat("a", 2000)
        symbolerror = try
            Avro.EnumSchema("E", [oversizedsymbol];
                            limits=Avro.Limits(max_name_bytes=1))
            nothing
        catch err
            err
        end
        @test symbolerror isa Avro.LimitError && symbolerror.limit == :max_name_bytes
        frozenenum = Avro.EnumSchema("Frozen", ["longsymbol", "other"])
        childerror = try
            Avro.ArraySchema(frozenenum;
                             limits=Avro.Limits(max_name_bytes=2))
            nothing
        catch err
            err
        end
        @test childerror isa Avro.LimitError && childerror.limit == :max_name_bytes
        @test_throws Avro.LimitError Avro.MapSchema(frozenenum;
                                                   limits=Avro.Limits(max_enum_symbols=1))
        @test_throws Avro.LimitError Avro.Field("abcde", Avro.IntSchema(); limits=Avro.Limits(max_name_bytes=4))
        @test_throws Avro.LimitError Avro.Field("a", Avro.IntSchema(); aliases=["abcde"], limits=Avro.Limits(max_name_bytes=4))
        aliased = Avro.Field("a", Avro.IntSchema(); aliases=["bc"])
        @test_throws Avro.LimitError Avro.RecordSchema("R"; fields=[aliased], limits=Avro.Limits(max_name_bytes=1))
        @test_throws Avro.LimitError Avro.RecordSchema("RecursiveDepth"; limits=Avro.Limits(max_schema_depth=1)) do ref
            return [Avro.Field("next", ref)]
        end
        @test_throws Avro.LimitError Avro.RecordSchema("RecursiveValues"; limits=Avro.Limits(max_total_values=1)) do ref
            return [Avro.Field("next", ref)]
        end
        bytebounded = Avro.Limits(max_schema_bytes=1024)
        @test_throws Avro.LimitError Avro.NullSchema(; props=(large=repeat("a", 2000),), limits=bytebounded)
        largefield = Avro.Field("x", Avro.StringSchema(); default=repeat("a", 2000))
        @test_throws Avro.LimitError Avro.RecordSchema("LargeDefault"; fields=[largefield], limits=bytebounded)
        let s = Avro.LongSchema(), lim = Avro.Limits(max_schema_depth=4)
            e = try
                for _ in 1:6
                    s = Avro.ArraySchema(s; limits=lim)
                end
                nothing
            catch err
                err
            end
            @test e isa Avro.LimitError && e.limit === :max_schema_depth
        end
        @test Avro.json(Avro.ArraySchema(Avro.LongSchema())) == "{\"type\":\"array\",\"items\":\"long\"}"
    end

    @testset "bounded charging printers (plan §4.4, amendment round 1)" begin
        s = P("{\"type\":\"record\",\"name\":\"BP\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"string\"}]}")
        tiny = Avro.Limits(max_schema_bytes=16)
        for f in (Avro.json, Avro.canonical, Avro.fingerprint)
            e = try
                f(s; limits=tiny)
                nothing
            catch err
                err
            end
            @test e isa Avro.LimitError && e.limit === :max_schema_bytes
        end
        exactwork = Avro.Limits(work_allowance=0)
        @test Avro.json(Avro.NullSchema(); limits=exactwork) == "\"null\""
        @test Avro.canonical(Avro.NullSchema(); limits=exactwork) == "\"null\""
        @test Avro.json(s) == Avro.json(Avro.parseschema(Avro.json(s)))                  # recorded-limits default round-trips
        @test occursin("BP", sprint(show, s))                                            # show never throws for admitted schemas
    end
end
