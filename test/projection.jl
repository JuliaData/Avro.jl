# The Phase 4d projection equivalence matrix (plan §6/§12): names, order, eltypes, Tables.schema, row
# count and values of `Table(src; select=cols)` equal `columntable(Table(src))[cols]` over the fixture
# battery — multi-block files, empty records, select=(), nested and nullable fields, directly and
# mutually recursive roots — in both validation modes and for ntasks ∈ {1, 2, 8}; malformed data inside
# projected-away fields follows each mode's documented policy; projected tables round-trip their schema.

@testset "Projection equivalence matrix (plan §6)" begin
    P = Avro.parseschema
    fixtures = Tuple{String,String,Vector{<:Any}}[]
    push!(fixtures, ("plain multi-block",
        "{\"type\":\"record\",\"name\":\"F1\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"score\",\"type\":[\"null\",\"double\"]},{\"name\":\"tag\",\"type\":{\"type\":\"enum\",\"name\":\"T1\",\"symbols\":[\"a\",\"b\",\"c\"]}}]}",
        [(id=Int64(i), name="n$i", score=i % 4 == 0 ? missing : i / 3, tag=("a", "b", "c")[i % 3 + 1]) for i in 1:2500]))
    push!(fixtures, ("nested and nullable",
        "{\"type\":\"record\",\"name\":\"F2\",\"fields\":[{\"name\":\"a\",\"type\":{\"type\":\"array\",\"items\":\"long\"}},{\"name\":\"m\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"r\",\"type\":{\"type\":\"record\",\"name\":\"Inner2\",\"fields\":[{\"name\":\"x\",\"type\":\"long\"},{\"name\":\"y\",\"type\":[\"null\",\"string\"]}]}},{\"name\":\"u\",\"type\":[\"long\",\"string\",\"null\"]}]}",
        [(a=Int64[i, i + 1], m=Dict("k$i" => "v$i"), r=(x=Int64(i), y=isodd(i) ? "s$i" : missing),
          u=i % 3 == 0 ? Avro.UnionValue(3, missing) : i % 3 == 1 ? Avro.UnionValue(1, Int64(i)) : Avro.UnionValue(2, "u$i")) for i in 1:600]))
    push!(fixtures, ("empty record", "{\"type\":\"record\",\"name\":\"F3\",\"fields\":[]}", [(;) for _ in 1:100]))
    push!(fixtures, ("directly recursive root",
        "{\"type\":\"record\",\"name\":\"Node\",\"fields\":[{\"name\":\"v\",\"type\":\"long\"},{\"name\":\"next\",\"type\":[\"null\",\"Node\"]}]}",
        [(v=Int64(i), next=i % 2 == 0 ? (v=Int64(-i), next=missing) : missing) for i in 1:400]))
    push!(fixtures, ("mutually recursive root",
        "{\"type\":\"record\",\"name\":\"A0\",\"fields\":[{\"name\":\"v\",\"type\":\"long\"},{\"name\":\"b\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"B0\",\"fields\":[{\"name\":\"w\",\"type\":\"string\"},{\"name\":\"a\",\"type\":[\"null\",\"A0\"]}]}]}]}",
        [(v=Int64(i), b=i % 3 == 0 ? missing : (w="w$i", a=missing)) for i in 1:400]))
    raised = Avro.Limits(max_total_bytes=2 << 30, max_block_bytes=16 << 20, max_block_output_bytes=256 << 20,
                         max_codec_memory=32 << 20, max_bytes=64 << 20, max_datum_bytes=64 << 20)
    selections(names) = begin
        sels = Vector{Vector{Symbol}}()
        push!(sels, Symbol[])                                 # select=()
        for nm in names
            push!(sels, [nm])                                 # every single column
        end
        length(names) > 1 && push!(sels, reverse(collect(names)))
        length(names) > 2 && push!(sels, [names[end], names[1]])
        sels
    end
    for (label, json, rows) in fixtures
        s = P(json)
        bytes = take!(Avro.tobuffer(rows; schema=s, block_bytes=512, limits=raised))
        @testset "$label" begin
            for val in (:strict, :fast)
                full = Avro.Table(IOBuffer(bytes); validate=val, limits=raised)
                fullct = Tables.columntable(full)
                fullsch = Tables.schema(full)
                for sel in selections(Tables.columnnames(full)), nt in (1, 2, 8)
                    pt = Avro.Table(IOBuffer(bytes); select=Tuple(sel), validate=val, ntasks=nt, limits=raised)
                    @test Tables.columnnames(pt) == sel
                    @test length(pt) == length(full)
                    psch = Tables.schema(pt)
                    @test collect(psch.names) == sel
                    for (k, nm) in enumerate(sel)
                        col = Tables.getcolumn(pt, k)
                        want = fullct[nm]
                        @test isequal(col, want) && typeof(col) == typeof(want)
                        @test psch.types[k] == fullsch.types[findfirst(==(nm), collect(fullsch.names))]
                    end
                    ps = Avro.schema(pt)
                    @test [f.name for f in ps.fields] == [String(nm) for nm in sel]
                end
                # writing a projection round-trips its derived schema (recursive roots included); for a
                # recursive root the projection is graph-wide, so nested records re-encode projected and
                # only the schema and row count round-trip value-independently
                names = collect(Tables.columnnames(full))
                isempty(names) && continue
                sel = [names[end]]
                pt = Avro.Table(IOBuffer(bytes); select=Tuple(sel), validate=val, limits=raised)
                io = IOBuffer()
                Avro.write(io, pt; limits=raised)
                seekstart(io)
                t2 = Avro.Table(io; limits=raised)
                @test Avro.json(Avro.schema(t2)) == Avro.json(Avro.schema(pt))
                @test length(t2) == length(pt)
                recursive = occursin("recursive", label)
                recursive || @test isequal(Tables.getcolumn(t2, 1), Tables.getcolumn(pt, 1))
            end
        end
    end
    @testset "malformed data inside projected-away fields (each mode's documented policy)" begin
        # a crafted container: {good: long, arr: array<boolean>} where arr uses the sized-block form
        # holding invalid boolean bytes — strict walks skipped fields (rejects), fast jumps the sized
        # block by its byte size (tolerates)
        s2 = P("{\"type\":\"record\",\"name\":\"M\",\"fields\":[{\"name\":\"good\",\"type\":\"long\"},{\"name\":\"arr\",\"type\":{\"type\":\"array\",\"items\":\"boolean\"}}]}")
        sync = collect(UInt8(1):UInt8(16))
        base = IOBuffer()
        w = Avro.Writer(base, s2; sync=sync)
        push!(w, (good=Int64(1), arr=[true]))
        close(w)
        orig = take!(base)
        r = Avro.Reader(IOBuffer(orig))
        e1 = Avro.prescanblocks(r).entries[1]
        close(r)
        headerend = e1.offset - Avro.varintlength(1) - Avro.varintlength(e1.size) - 1
        payload = UInt8[0x02, 0x03, 0x04, 0x02, 0x02, 0x00]     # good=1; arr sized block(count=-2, 2 bytes): two invalid bools; end
        crafted = vcat(orig[1:headerend], UInt8[0x02], UInt8[UInt8(2 * length(payload))], payload, sync)
        pt = Avro.Table(IOBuffer(crafted); select=(:good,), validate=:fast)
        @test Tables.getcolumn(pt, :good) == [1]
        @test_throws Avro.DataError Avro.Table(IOBuffer(crafted); select=(:good,), validate=:strict)
        @test_throws Avro.DataError Avro.Table(IOBuffer(crafted); select=(:arr,), validate=:fast)   # selected data is always validated
        rs = Avro.Rows(IOBuffer(crafted); select=(:good,), validate=:fast)
        @test [row.good for row in rs] == [1]
        close(rs)
        rst = Avro.Rows(IOBuffer(crafted); select=(:good,), validate=:strict)
        @test_throws Avro.DataError collect(rst)
        close(rst)
        # an invalid skipped value outside a sized block is rejected in both modes (bools are
        # domain-checked when skipped; only sized-block jumps and skipped-string UTF-8 are relaxed)
        s3 = P("{\"type\":\"record\",\"name\":\"M2\",\"fields\":[{\"name\":\"good\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"boolean\"}]}")
        rows3 = [(good=Int64(i), b=iseven(i)) for i in 1:10]
        bytes3 = take!(Avro.tobuffer(rows3; schema=s3, block_bytes=64))
        r3 = Avro.Reader(IOBuffer(bytes3))
        f1 = Avro.prescanblocks(r3).entries[1]
        close(r3)
        bad3 = copy(bytes3)
        bad3[f1.offset + 1] = 0x02                              # row 1's boolean byte
        @test_throws Avro.DataError Avro.Table(IOBuffer(bad3); select=(:good,), validate=:strict)
        @test_throws Avro.DataError Avro.Table(IOBuffer(bad3); select=(:good,), validate=:fast)
        # invalid UTF-8 inside a skipped string is tolerated in both modes; selecting it rejects
        s4 = P("{\"type\":\"record\",\"name\":\"M3\",\"fields\":[{\"name\":\"good\",\"type\":\"long\"},{\"name\":\"s\",\"type\":\"string\"}]}")
        bytes4 = take!(Avro.tobuffer([(good=Int64(7), s="AB")]; schema=s4))
        r4 = Avro.Reader(IOBuffer(bytes4))
        g1 = Avro.prescanblocks(r4).entries[1]
        close(r4)
        bad4 = copy(bytes4)
        @test bad4[g1.offset + 2] == UInt8('A')
        bad4[g1.offset + 2] = 0xff                              # invalid UTF-8, length intact
        for val in (:strict, :fast)
            @test Tables.getcolumn(Avro.Table(IOBuffer(bad4); select=(:good,), validate=val), :good) == [7]
            @test_throws Avro.DataError Avro.Table(IOBuffer(bad4); select=(:s,), validate=val)
        end
    end

    @testset "corpus sweep: every generated record-root fixture, Table and Rows (R14)" begin
        gen = joinpath(@__DIR__, "fixtures", "generated")
        raised = Avro.Limits(max_total_bytes=2 << 30, max_block_bytes=16 << 20, max_block_output_bytes=256 << 20,
                             max_codec_memory=32 << 20, max_bytes=64 << 20, max_datum_bytes=64 << 20)
        for avsc in filter(f -> endswith(f, ".avsc"), readdir(joinpath(gen, "schemas"); join=true))
            s = Avro.parseschema(read(avsc, String))
            s isa Avro.RecordSchema || continue
            data = joinpath(gen, "data", basename(avsc)[1:end - 5] * "-null.avro")
            isfile(data) || continue
            full = Avro.Table(data; limits=raised)
            names = collect(Tables.columnnames(full))
            isempty(names) && continue
            fullct = Tables.columntable(full)
            sels = Vector{Symbol}[[names[1]], [names[end], names[1]]]
            length(names) > 2 && push!(sels, names[1:2:end])
            for sel in sels, val in (:strict, :fast), nt in (1, 8)
                pt = Avro.Table(data; select=Tuple(sel), validate=val, ntasks=nt, limits=raised)
                @test Tables.columnnames(pt) == sel && length(pt) == length(full)
                for (k, nm) in enumerate(sel)
                    got = Tables.getcolumn(pt, k)
                    want = fullct[nm]
                    @test isequal(got, want) && typeof(got) == typeof(want)
                end
                rl = Avro.Rows(data; select=Tuple(sel), validate=val, limits=raised)
                rows = collect(rl)
                close(rl)
                @test length(rows) == length(full)
                for (k, nm) in enumerate(sel)
                    @test isequal([Tables.getcolumn(r, k) for r in rows], collect(fullct[nm]))
                end
            end
        end
    end
end
