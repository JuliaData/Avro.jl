# Small inputs that once cost far more than their size, and inputs that once escaped the `AvroError`
# hierarchy. Each attack here finishes only because the work is linear: the exponential ones would not
# return at all.

hostileallocated(f) = @allocated f()

function hostilecontainer(schemajson, blocks; codec=nothing)
    zz(n) = Avro.encode(Avro.LongSchema(), Int64(n))
    str(s) = vcat(zz(sizeof(s)), Vector{UInt8}(codeunits(s)))
    sync = fill(0x42, 16)
    out = UInt8['O', 'b', 'j', 0x01]
    append!(out, zz(codec === nothing ? 1 : 2), str("avro.schema"), str(schemajson))
    codec === nothing || append!(out, str("avro.codec"), str(codec))
    push!(out, 0x00)
    append!(out, sync)
    for (count, payload) in blocks
        append!(out, zz(count), zz(length(payload)), payload, sync)
    end
    return out
end

@testset "Hostile inputs" begin
    P = Avro.parseschema
    zz(n) = Avro.encode(Avro.LongSchema(), Int64(n))

    @testset "defaults are checked once per (schema, default) pair" begin
        # every record has two fields of the next record, each defaulting to {}
        doubling = """{"type":"record","name":"R0","fields":[]}"""
        for k in 1:60
            doubling = """{"type":"record","name":"R$k","fields":[{"name":"a","type":$doubling,"default":{}},{"name":"b","type":"R$(k - 1)","default":{}}]}"""
        end
        s = P(doubling)
        @test s isa Avro.RecordSchema && Avro.minsize(s) == 0
        # two record branches at every level and a default that fails only at the bottom
        backtracking = "\"int\""
        for k in 1:30
            field(t) = """{"name":"n","type":$t}"""
            lower = k == 1 ? "\"int\"" : """["A$(k - 1)","B$(k - 1)"]"""
            backtracking = """[{"type":"record","name":"A$k","fields":[$(field(backtracking))]},{"type":"record","name":"B$k","fields":[$(field(lower))]}]"""
        end
        baddefault = "\"bad\""
        for _ in 1:30
            baddefault = """{"n":$baddefault}"""
        end
        @test_throws Avro.SchemaError P("""{"type":"record","name":"Root","fields":[{"name":"f","type":$backtracking,"default":$baddefault}]}""")
        gooddefault = "1"
        for _ in 1:30
            gooddefault = """{"n":$gooddefault}"""
        end
        rooted = P("""{"type":"record","name":"Root","fields":[{"name":"f","type":$backtracking,"default":$gooddefault}]}""")
        @test rooted.fields[1].default.branch == 1 && rooted.fields[1].default.valid
        # an object with many members looks each field up in constant time
        fields = join(("""{"name":"f$i","type":"int"}""" for i in 1:20_000), ",")
        members = join(("\"f$i\":$i" for i in 1:20_000), ",")
        wide = P("""{"type":"record","name":"Outer","fields":[{"name":"w","type":{"type":"record","name":"Wide","fields":[$fields]},"default":{$members}}]}""")
        @test wide.fields[1].default.valid
        # a recursive default has no finite expansion, by any path
        @test_throws Avro.SchemaError P("""{"type":"record","name":"L","fields":[{"name":"n","type":"L","default":{}}]}""")
    end

    @testset "Java's structural matching decides each record pair once" begin
        reader = P("""{"type":"record","name":"A","fields":[{"name":"n","type":["A",{"type":"record","name":"B","fields":[{"name":"n","type":["A","B"]}]}]}]}""")
        chain = """{"type":"record","name":"W60","fields":[]}"""
        for i in 59:-1:1
            chain = """{"type":"record","name":"W$i","fields":[{"name":"n","type":$chain}]}"""
        end
        writer = """{"type":"record","name":"A","fields":[{"name":"n","type":$chain}]}"""
        file = hostilecontainer(writer, [])
        @test_throws Avro.AvroError Avro.Table(file; reader_schema=reader, union_resolution=:java)
    end

    @testset "named references cannot nest past the depth limit" begin
        function siblingchain(n, tail="")
            fields = ["""{"name":"f0","type":{"type":"record","name":"R0","fields":[]}}"""]
            for k in 1:n
                push!(fields, """{"name":"f$k","type":{"type":"record","name":"R$k","fields":[{"name":"x","type":"R$(k - 1)"}]}}""")
            end
            isempty(tail) || push!(fields, tail)
            return """{"type":"record","name":"Root","fields":[$(join(fields, ","))]}"""
        end
        for n in (5_000, 100_000)
            err = try
                P(siblingchain(n))
                nothing
            catch caught
                caught
            end
            @test err isa Avro.LimitError && err.limit === :max_depth
        end
        @test_throws Avro.LimitError Avro.Reader(hostilecontainer(siblingchain(30_000, """{"name":"a","type":{"type":"array","items":"R30000"}}"""), []))
        # within the limit, the schema prints as JSON that parses under the same limit
        fine = P(siblingchain(300))
        @test P(Avro.json(fine)) == fine
        limits = Avro.Limits(max_depth=64)
        @test_throws Avro.LimitError P(siblingchain(100); limits=limits)
        @test_throws Avro.LimitError P(Avro.json(P(siblingchain(30))); limits=limits)
        # every array's plan reads a size computed once for the graph
        many = siblingchain(300, join(("""{"name":"a$i","type":{"type":"array","items":"R$(i % 300)"}}""" for i in 1:5_000), ","))
        @test length(Avro.readplan(P(many)).fields) == 5_301
        # a schema the reader would refuse is refused by the writer first
        deep = "\"long\""
        for _ in 1:400
            deep = """{"type":"record","name":"D$(hash(deep))","fields":[{"name":"x","type":$deep}]}"""
        end
        deepschema = P(deep; limits=Avro.Limits(max_depth=2000))
        @test_throws Avro.LimitError Avro.Writer(IOBuffer(), deepschema)
    end

    @testset "claims are checked before they allocate" begin
        # nested arrays each claiming every remaining byte
        schema = "\"long\""
        for _ in 1:200
            schema = """{"type":"array","items":$schema}"""
        end
        s = P(schema)
        payload = zeros(UInt8, 1 << 20)
        claims = reduce(vcat, [zz(length(payload)) for _ in 1:200])
        bytes = vcat(claims, payload)
        function nestedclaims(b)
            try
                Avro.decode(s, b)
            catch err
                err isa Avro.AvroError || rethrow()
            end
            return nothing
        end
        nestedclaims(bytes)
        @test hostileallocated(() -> nestedclaims(bytes)) < 64 << 20
        # a second block claiming far more rows than the file could hold
        rec = """{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}"""
        for claim in (Int64(1) << 28, Int64(1) << 45, Int64(1) << 61)
            file = hostilecontainer(rec, [(1, zz(1)), (claim, zz(2))])
            rows = Avro.Rows(file)
            @test length(rows) == 1 + claim                 # the count is the headers' claim
            @test Base.IteratorSize(typeof(rows)) === Base.SizeUnknown()
            @test_throws Avro.AvroError collect(rows)
            @test_throws Avro.AvroError Avro.Table(file)
            @test_throws Avro.AvroError map(identity, Avro.Rows(file))
        end
        # zero-width columns claiming more rows than memory can address
        nulls = """{"type":"record","name":"N","fields":[{"name":"a","type":"null"},{"name":"b","type":"null"}]}"""
        for blocks in ([(typemax(Int64), UInt8[])], [(typemax(Int64) ÷ 2, UInt8[]), (typemax(Int64) ÷ 2, UInt8[]), (typemax(Int64) ÷ 2, UInt8[])])
            file = hostilecontainer(nulls, blocks)
            for source in (file, IOBuffer(file))
                @test_throws Avro.AvroError Avro.Table(source)
            end
        end
        # one namespace repeated through thousands of aliases and types
        namespace = "n" ^ (300 << 10)
        aliases = join(("\"a$i\"" for i in 1:4000), ",")
        @test_throws Avro.LimitError P("""{"type":"record","name":"R","namespace":"$namespace","aliases":[$aliases],"fields":[]}""")
        named = join(("""{"name":"f$i","type":{"type":"enum","name":"E$i","symbols":["A"]}}""" for i in 1:4000), ",")
        @test_throws Avro.LimitError P("""{"type":"record","name":"R","namespace":"$namespace","fields":[$named]}""")
    end

    @testset "many blocks over many columns" begin
        fields = join(("""{"name":"c$i","type":"null"}""" for i in 1:4000), ",")
        schema = """{"type":"record","name":"Wide","fields":[$fields]}"""
        file = hostilecontainer(schema, [(0, UInt8[]) for _ in 1:4000])
        for source in (() -> file, () -> IOBuffer(file))
            read!() = Avro.Table(source())
            read!()
            @test hostileallocated(read!) < 64 << 20
            @test length(read!()) == 0 && length(Tables.columnnames(read!())) == 4000
        end
        rows = hostilecontainer("""{"type":"record","name":"F","fields":[{"name":"a","type":"long"}]}""", [(1, zz(i)) for i in 1:100])
        @test Avro.Table(IOBuffer(rows)).a == 1:100 == Avro.Table(rows).a
        @test Tables.columntable(Avro.Rows(IOBuffer(rows))).a == 1:100
    end

    @testset "a writer union resolved into a reader record" begin
        record = """{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}"""
        writer = P("""["null",$record]""")
        reader = P(record)
        file = take!(Avro.tobuffer([(a=1, b="x"), (a=2, b="y")]; schema=writer))
        @test length(collect(Avro.Rows(file; reader_schema=reader))) == 2
        t = Avro.Table(file; reader_schema=reader)
        @test t.a == [1, 2] && t.b == ["x", "y"]
        @test Avro.Table(file; reader_schema=reader, select=:b).b == ["x", "y"]
        @test Avro.Table(IOBuffer(file); reader_schema=reader, select=:b).b == ["x", "y"]
        @test [r.b for r in Avro.Rows(file; reader_schema=reader, select=:b)] == ["x", "y"]
        withnull = take!(Avro.tobuffer([missing]; schema=writer))
        @test_throws Avro.AvroError Avro.Table(withnull; reader_schema=reader)
    end

    @testset "conversion failures on file content are ConversionErrors" begin
        str = P("\"string\"")
        nul = Avro.encode(str, "a\0b")
        @test_throws Avro.ConversionError Avro.decode(str, nul, Symbol)
        m = P("""{"type":"map","values":"long"}""")
        @test_throws Avro.ConversionError Avro.decode(m, Avro.encode(m, Dict("a\0b" => 1)), Dict{Symbol,Int64})
        long = P("\"long\"")
        @test_throws Avro.ConversionError Avro.decode(long, Avro.encode(long, typemax(Int64)), Char)
        dbl = P("\"double\"")
        @test_throws Avro.ConversionError Avro.decode(dbl, Avro.encode(dbl, 1e300), DataDecimals.DecimalValue{Int128})
        @test_throws Avro.ConversionError Avro.DatumReader(P("""{"type":"record","name":"R","fields":[]}"""), @NamedTuple{a::Int64})
        # narrowing numeric targets
        @test_throws Avro.ConversionError Avro.decode(dbl, Avro.encode(dbl, 1e300), Float32)
        @test Avro.decode(dbl, Avro.encode(dbl, 1.5), Float32) === 1.5f0
        @test Avro.decode(dbl, Avro.encode(dbl, Inf), Float32) === Inf32
        @test isnan(Avro.decode(dbl, Avro.encode(dbl, NaN), Float32))
        @test_throws Avro.ConversionError Avro.decode(long, Avro.encode(long, 2^53 + 1), Float64)
        @test Avro.decode(long, Avro.encode(long, 2^53), Float64) === 2.0^53
        file = take!(Avro.tobuffer([1]))
        @test_throws Avro.SchemaError Avro.Table(file)
        @test_throws Avro.SchemaError Avro.Table(take!(Avro.tobuffer([(a=1,)])); select=:b)
    end

    @testset "empty blocks cannot multiply the value allowance" begin
        varint(n) = Avro.encode(Avro.LongSchema(), Int64(n))
        nullrecord = P("""{"type":"record","name":"N","fields":[{"name":"n","type":"null"}]}""")
        io = IOBuffer()
        close(Avro.Writer(io, nullrecord; sync=zeros(UInt8, 16)))
        header = take!(io)
        emptyblock(count) = vcat(varint(count), varint(0), zeros(UInt8, 16))
        cap = Avro.blockvaluecap(Avro.Limits(), 0)                 # an 18-byte block: 16 values per file byte
        @test cap == 16 * 18
        @test length(Avro.Table(vcat(header, emptyblock(cap ÷ 2)))) == cap ÷ 2
        hostile = vcat(header, repeat(emptyblock(65_536), 64))     # 1 KiB claiming four million zero-byte datums
        errs = [try; Avro.Table(hostile; ntasks=n); nothing; catch e; e; end for n in (1, 4)]
        @test all(e -> e isa Avro.LimitError && e.limit === :max_values_per_byte, errs)
        @test sprint(showerror, errs[1]) == sprint(showerror, errs[2])
        @test_throws Avro.LimitError collect(Avro.Rows(hostile))
        # the writer splits zero-byte datums into blocks a default reader accepts
        many = Avro.tobuffer([(n=missing,) for _ in 1:2_000]; schema=nullrecord)
        bytes = take!(copy(many))
        @test all(b -> first(b) <= cap, Avro.Reader(r -> collect(Avro.eachblock(r)), bytes))
        @test length(Avro.Table(bytes)) == 2_000
    end

    @testset "a read's decompressed output is bounded by its input" begin
        s = P("""{"type":"record","name":"Z","fields":[{"name":"z","type":"long"}]}""")
        zeros_ = [(z=Int64(0),) for _ in 1:2_000_000]                # one byte per row, compresses about 500 times
        lax = Avro.Limits(max_inflation=typemax(Int))
        file = take!(Avro.tobuffer(zeros_; schema=s, codec=:deflate, limits=lax))
        @test length(Avro.Table(file)) == 2_000_000                  # within the default budget
        tight = Avro.Limits(max_bytes=1 << 20, max_inflation=2)
        errs = [try; Avro.Table(file; ntasks=n, limits=tight); nothing; catch e; e; end for n in (1, 4)]
        @test all(e -> e isa Avro.LimitError && e.limit === :max_inflation, errs)
        @test sprint(showerror, errs[1]) == sprint(showerror, errs[2])
        @test errs[1].observed > errs[1].value > 1 << 20              # the budget grew with the compressed bytes read
        @test_throws Avro.LimitError Avro.Table(file; mmap=false, limits=tight)
        @test_throws Avro.LimitError collect(Avro.Rows(file; limits=tight))
        @test any(issue -> occursin("max_inflation", issue), Avro.inspect(file; limits=tight).issues)
        # parallel workers stop near the whole file's bound rather than decompressing every block first
        bomb = take!(Avro.tobuffer((x=fill(zeros(UInt8, 1 << 20), 200),); codec=:zstandard))
        small = Avro.Limits(max_bytes=2 << 20, max_inflation=2)
        bombread() = try; Avro.Table(bomb; ntasks=4, limits=small); nothing; catch e; e; end
        bombread()
        work = @timed bombread()
        @test work.value isa Avro.LimitError && work.value.limit === :max_inflation
        @test work.bytes < 100 << 20                                  # over 400 MiB when every block decompresses
        # the writer refuses to emit what its own limits could not read back
        writeerr = try; Avro.tobuffer(zeros_; schema=s, codec=:deflate, limits=tight); nothing; catch e; e; end
        @test writeerr isa Avro.LimitError && writeerr.limit === :max_inflation
        @test_throws ArgumentError Avro.Limits(max_inflation=0)
    end

    @testset "a NUL in a field name matches a dictionary key" begin
        nulfield = P("{\"type\":\"record\",\"name\":\"N\",\"fields\":[{\"name\":\"a\\u0000b\",\"type\":\"long\"}]}";
                     allow_invalid_names=true)
        @test Tables.getcolumn(Avro.decode(nulfield, Avro.encode(nulfield, Dict("a\0b" => 7))), 1) == 7
    end

    @testset "byte sources that are not contiguous" begin
        wide = P("""{"type":"bytes","logicalType":"decimal","precision":50,"scale":0}""")
        value = Avro.WideDecimal(big(10)^45 + 5, 0)
        bytes = Avro.encode(wide, value)
        stepped = zeros(UInt8, 2 * length(bytes))
        stepped[1:2:end] .= bytes
        @test Avro.decode(wide, view(stepped, 1:2:length(stepped))) == value
        @test Avro.decode(wide, view(reverse(bytes), length(bytes):-1:1)) == value
        s = P("\"string\"")
        text = Avro.encode(s, "hello")
        @test Avro.decode(s, view(reverse(text), length(text):-1:1)) == "hello"
        file = take!(Avro.tobuffer([(a=i,) for i in 1:3]))
        @test Avro.Table(view(file, :)).a == 1:3
        @test Avro.Table(view(file, 1:length(file))).a == 1:3
        @test [r.a for r in Avro.Rows(view(file, :))] == 1:3
    end

    @testset "iteration after a failure" begin
        rec = """{"type":"record","name":"R","fields":[{"name":"s","type":"string"}]}"""
        good(i) = Avro.encode(P(rec), (s="v$i",))
        bad = vcat(zz(100), UInt8[0x41])                   # a string claiming more than its block holds
        file = hostilecontainer(rec, [(1, good(1)), (2, vcat(bad, good(2))), (1, good(3))])
        for iterator in (rows -> rows, identity)
            rows = Avro.Rows(file)
            next = iterate(rows)
            @test next[1].s == "v1"
            @test_throws Avro.DataError iterate(rows)
            next = iterate(rows)                           # the bad block is gone; the next one reads
            @test next[1].s == "v3"
            @test iterate(rows) === nothing
        end
        datums = Avro.Reader(file) do r
            it = Avro.eachdatum(r)
            first = iterate(it)[1].s
            failed = try
                iterate(it)
                false
            catch err
                err isa Avro.DataError
            end
            return (first, failed, iterate(it)[1].s, iterate(it))
        end
        @test datums == ("v1", true, "v3", nothing)
        # damaged framing leaves no next block to find
        torn = hostilecontainer(rec, [(1, good(1)), (1, good(2))])
        torn[end - 3] = 0x00
        rows = Avro.Rows(IOBuffer(torn); mmap=false)
        @test iterate(rows)[1].s == "v1"
        @test_throws Avro.DataError iterate(rows)
        @test_throws ArgumentError iterate(rows)
    end
end
