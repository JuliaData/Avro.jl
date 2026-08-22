@testset "Sort order (compare / comparebytes)" begin
    P = Avro.parseschema
    @testset "the Java sort-order vectors (BinaryData.compare is the spec column)" begin
        dir = joinpath(FIXTURES, "generated", "sortorder")
        verdicts = Dict{String,Tuple{String,String}}()
        for line in eachline(joinpath(dir, "verdicts.tsv"))
            f = split(line, '\t')
            verdicts[f[1]] = (String(f[2]), String(f[3]))
        end
        @test length(verdicts) == 51
        for case in sort(collect(keys(verdicts)))
            text = read(joinpath(dir, "$case.avsc"), String)
            s = P(text)
            praw = P(replace(text, r",\s*\"logicalType\":\s*\"[^\"]+\"" => ""))   # the plain underlying schema keeps the wire form as written
            ja = read(joinpath(dir, "$case.a.json"), String)
            jb = read(joinpath(dir, "$case.b.json"), String)
            generic, binary = verdicts[case]
            if startswith(binary, "ERROR")
                @test_throws ArgumentError Avro.compare(s, Avro.fromjson(s, ja), Avro.fromjson(s, jb))
                continue
            end
            expected = parse(Int, binary)
            rawa = Avro.encode(praw, Avro.fromjson(praw, ja))
            rawb = Avro.encode(praw, Avro.fromjson(praw, jb))
            @test Avro.comparebytes(s, rawa, rawb) == expected                  # the spec order on the encodings as written
            @test Avro.comparebytes(s, rawb, rawa) == -expected
            @test Avro.comparebytes(s, rawa, rawa) == 0
            @test Avro.comparebytes(s, rawa, rawb; validate=:fast) == expected
            a = Avro.fromjson(s, ja)
            b = Avro.fromjson(s, jb)
            @test Avro.compare(s, a, b) == Avro.comparebytes(s, Avro.encode(s, a), Avro.encode(s, b))   # the cross-API contract on canonical encodings
        end
        # the documented divergences: non-minimal decimals and mixed-case uuids decode equal, so `compare` is 0
        for case in ("case47", "case48", "case49")
            s = P(read(joinpath(dir, "$case.avsc"), String))
            @test Avro.compare(s, Avro.fromjson(s, read(joinpath(dir, "$case.a.json"), String)), Avro.fromjson(s, read(joinpath(dir, "$case.b.json"), String))) == 0
        end
    end
    @testset "cross-form arrays compare by items, not blocks" begin
        dir = joinpath(FIXTURES, "generated", "blocking", "crossform")
        s = P(read(joinpath(dir, "arr.avsc"), String))
        forms = Dict(name => (read(joinpath(dir, "$name.positive.bin")), read(joinpath(dir, "$name.sized.bin"))) for name in ("a", "a2", "b"))
        for (name, (pos, sized)) in forms
            @test pos != sized                                                  # genuinely different encodings
            @test Avro.comparebytes(s, pos, sized) == 0
            @test Avro.comparebytes(s, sized, pos) == 0
        end
        c = Avro.comparebytes(s, forms["a"][1], forms["b"][1])
        @test c != 0
        @test Avro.comparebytes(s, forms["a"][2], forms["b"][2]) == c
        @test Avro.comparebytes(s, forms["a"][1], forms["b"][2]) == c
    end
    @testset "records, orders, unions, prefixes" begin
        rs = P("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":\"int\",\"order\":\"ignore\"},{\"name\":\"m\",\"type\":{\"type\":\"map\",\"values\":\"int\"},\"order\":\"ignore\"},{\"name\":\"b\",\"type\":\"string\",\"order\":\"descending\"}]}")
        va = (a=9, m=Dict("k" => 1), b="a")
        vb = (a=1, m=Dict("q" => 2), b="b")
        @test Avro.compare(rs, va, vb) == 1 && Avro.compare(rs, vb, va) == -1 && Avro.compare(rs, va, va) == 0
        @test_throws ArgumentError Avro.compare(P("{\"type\":\"map\",\"values\":\"int\"}"), Dict("a" => 1), Dict("a" => 1))
        @test_throws ArgumentError Avro.compare(P("[\"null\",{\"type\":\"map\",\"values\":\"int\"}]"), missing, missing)
        us = P("[\"int\",\"string\"]")
        @test Avro.compare(us, Avro.UnionValue(1, Int32(500)), Avro.UnionValue(2, "a")) == -1
        @test Avro.compare(us, Avro.UnionValue(2, "a"), Avro.UnionValue(2, "b")) == -1
        arr = P("{\"type\":\"array\",\"items\":\"string\"}")
        @test Avro.compare(arr, ["ab"], ["a", "b"]) == 1                        # item-wise: "ab" > "a"
        @test Avro.compare(arr, String[], ["a"]) == -1
        rec = P("{\"type\":\"record\",\"name\":\"N\",\"fields\":[{\"name\":\"v\",\"type\":\"long\"},{\"name\":\"next\",\"type\":[\"null\",\"N\"],\"default\":null}]}")
        @test Avro.compare(rec, (v=1, next=(v=5, next=missing)), (v=1, next=(v=2, next=missing))) == 1
    end
    @testset "exact consumption, budgets, agreement property" begin
        s = P("\"long\"")
        good = Avro.encode(s, 1)
        @test_throws Avro.DataError Avro.comparebytes(s, vcat(good, UInt8[0x00]), good)
        @test_throws Avro.DataError Avro.comparebytes(s, good, vcat(good, UInt8[0x00]))
        @test_throws Avro.DataError Avro.comparebytes(s, UInt8[], good)
        big = P("{\"type\":\"array\",\"items\":\"boolean\"}")
        bytes = Avro.encode(big, fill(true, 100_000))
        @test_throws Avro.LimitError Avro.comparebytes(big, bytes, bytes; limits=Avro.Limits(max_total_values=1000))
        rng = Random.Xoshiro(20260823)
        orderable = ["\"boolean\"", "\"int\"", "\"long\"", "\"float\"", "\"double\"", "\"bytes\"", "\"string\"",
                     "{\"type\":\"int\",\"logicalType\":\"date\"}", "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":9,\"scale\":2}",
                     "{\"type\":\"string\",\"logicalType\":\"uuid\"}", "{\"type\":\"enum\",\"name\":\"OE\",\"symbols\":[\"a\",\"b\",\"c\"]}",
                     "{\"type\":\"fixed\",\"name\":\"OF\",\"size\":3}", "[\"null\",\"long\"]", "{\"type\":\"array\",\"items\":\"int\"}"]
        checked = 0
        for (i, leaf) in enumerate(orderable)
            s = P("{\"type\":\"record\",\"name\":\"Prop$i\",\"fields\":[{\"name\":\"x\",\"type\":$leaf},{\"name\":\"y\",\"type\":\"int\",\"order\":\"descending\"}]}")
            for _ in 1:40
                a = samplevalue(s, rng)
                b = samplevalue(s, rng)
                ea, eb = Avro.encode(s, a), Avro.encode(s, b)
                c = Avro.comparebytes(s, ea, eb)
                @test Avro.compare(s, a, b) == c
                @test Avro.comparebytes(s, eb, ea) == -c
                checked += 1
            end
        end
        @test checked == 40 * length(orderable)
    end
end
