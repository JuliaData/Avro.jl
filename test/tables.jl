import DataAPI

@testset "Tables integration" begin
    P = Avro.parseschema
    rows = [(a=Int64(i), b="r$i", c=i % 3 == 0 ? missing : i / 2, e=isodd(i) ? "X" : "Y") for i in 1:250]
    s = P("{\"type\":\"record\",\"name\":\"T\",\"namespace\":\"t\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"string\"},{\"name\":\"c\",\"type\":[\"null\",\"double\"]},{\"name\":\"e\",\"type\":{\"type\":\"enum\",\"name\":\"XY\",\"symbols\":[\"X\",\"Y\"]}}]}")
    buf() = Avro.tobuffer(rows; schema=s, codec=:deflate, block_bytes=128, metadata=Dict("k" => Vector{UInt8}("v")))
    @testset "Table basics, Tables and DataAPI interfaces" begin
        t = Avro.Table(buf())
        @test length(t) == 250
        @test Tables.istable(typeof(t)) && Tables.columnaccess(typeof(t))
        @test Tables.columnnames(t) == [:a, :b, :c, :e]
        ts = Tables.schema(t)
        @test ts isa Tables.Schema{nothing,nothing}                            # stored: nothing in type parameters
        @test collect(ts.names) == [:a, :b, :c, :e]
        @test collect(ts.types) == [Int64, String, Union{Missing,Float64}, Avro.EnumValue]
        @test Tables.getcolumn(t, 1) == 1:250 && Tables.getcolumn(t, :a) isa Vector{Int64}
        @test Tables.getcolumn(t, :b) == ["r$i" for i in 1:250]
        @test isequal(Tables.getcolumn(t, :c), [i % 3 == 0 ? missing : i / 2 for i in 1:250])
        @test Tables.getcolumn(t, :c) isa Vector{Union{Missing,Float64}}
        @test Tables.getcolumn(t, :e) isa Vector{Avro.EnumValue} && String(Tables.getcolumn(t, :e)[1]) == "X"
        @test Avro.schema(t) isa Avro.RecordSchema && Avro.fullname(Avro.schema(t)) == "t.T"
        @test Avro.codec(t) === :deflate && Avro.writerschema(t) === Avro.schema(t)
        parts = collect(Tables.partitions(t))
        @test length(parts) > 1 && sum(length, parts) == 250
        @test Tables.getcolumn(parts[1], :a)[1] == 1 && Tables.getcolumn(parts[end], :a)[end] == 250
        @test DataAPI.metadatasupport(Avro.Table) == (read=true, write=false)
        @test "k" in collect(DataAPI.metadatakeys(t))
        @test DataAPI.metadata(t, "k") == "v"
        @test DataAPI.metadata(t, "k"; style=true) == ("v", :default)
        @test DataAPI.metadata(t, "nope", 42) == 42
        @test_throws KeyError DataAPI.metadata(t, "nope")
        @test occursin("250 rows", sprint(show, t))
        arrio = IOBuffer()
        w = Avro.Writer(arrio, P("\"long\""))
        push!(w, 1)
        close(w)
        seekstart(arrio)
        @test_throws ArgumentError Avro.Table(arrio)
    end
    @testset "Table == columntable(Rows); the three Rows modes" begin
        t = Avro.Table(buf())
        r = Avro.Rows(buf())
        @test Tables.istable(r) && Tables.rowaccess(r) && Tables.rows(r) === r
        ct = Tables.columntable(r)
        close(r)
        @test collect(keys(ct)) == [:a, :b, :c, :e]
        for k in (:a, :b, :c, :e)
            @test isequal(collect(ct[k]), Tables.getcolumn(t, k))
        end
        r2 = Avro.Rows(buf())
        row1, _ = iterate(r2)
        @test row1 isa Avro.Row && row1.a === Int64(1) && Tables.getcolumn(row1, :b) == "r1" && Tables.getcolumn(row1, 2) == "r1"
        @test Tables.columnnames(row1) == [:a, :b, :c, :e]
        @test Avro.record(row1) isa Avro.Record
        close(r2)
        rec = Avro.decode(s, Avro.encode(s, rows[1]))
        @test Avro.Row(rec).a === Int64(1)
        rt = Avro.Rows(buf(); T=NamedTuple{(:a, :b, :c, :e),Tuple{Int64,String,Union{Missing,Float64},String}})
        @test !Tables.istable(rt)
        vals = collect(rt)
        close(rt)
        @test length(vals) == 250 && vals[1].a == 1 && vals[1].e == "X" && vals[2].e == "Y"
        rt2 = Avro.Rows(buf(); T=NamedTuple{(:a,),Tuple{Int64}})
        @test_throws ArgumentError Tables.partitions(rt2)
        close(rt2)
        arrio = IOBuffer()
        w = Avro.Writer(arrio, P("\"long\""))
        push!(w, 1)
        push!(w, 2)
        close(w)
        seekstart(arrio)
        rn = Avro.Rows(arrio)
        @test !Tables.istable(rn)
        @test collect(rn) == [1, 2]
        close(rn)
        rp = Avro.Rows(buf())
        pts = collect(Tables.partitions(rp))
        close(rp)
        @test sum(length, pts) == 250 && all(p -> p isa Avro.Table, pts) && length(pts) > 1
        @test Avro.Rows(rr -> sum(row.a for row in rr), buf()) == sum(1:250)
    end
    @testset "select= projection" begin
        t = Avro.Table(buf())
        full = Tables.columntable(t)
        for sel in ((:e, :a), (:b,), (:a, :b, :c, :e), ())
            pt = Avro.Table(buf(); select=sel)
            @test Tables.columnnames(pt) == collect(sel)
            @test length(pt) == 250
            for k in sel
                @test isequal(Tables.getcolumn(pt, k), full[k])
                @test typeof(Tables.getcolumn(pt, k)) == typeof(full[k])
            end
            ps = Avro.schema(pt)
            @test [f.name for f in ps.fields] == [String(k) for k in sel]
            if !isempty(sel)
                io = IOBuffer()
                Avro.write(io, pt)
                seekstart(io)
                t2 = Avro.Table(io)
                @test Avro.json(Avro.schema(t2)) == Avro.json(ps)               # projected tables round-trip their derived schema
                for k in sel
                    @test isequal(Tables.getcolumn(t2, k), Tables.getcolumn(pt, k))
                end
            end
        end
        @test_throws ArgumentError Avro.Table(buf(); select=(:nope,))
        @test_throws ArgumentError Avro.Table(buf(); select=(:a, :a))
        pr = Avro.Rows(buf(); select=(:e, :a))
        row1, _ = iterate(pr)
        @test Tables.columnnames(row1) == [:e, :a] && row1.a === Int64(1) && String(row1.e) == "X"
        close(pr)
        ptf = Avro.Table(buf(); select=(:a,), validate=:fast)
        @test Tables.getcolumn(ptf, :a) == 1:250
    end
    @testset "reader_schema through Table and Rows" begin
        rdr = P("{\"type\":\"record\",\"name\":\"T\",\"namespace\":\"t\",\"fields\":[{\"name\":\"a\",\"type\":\"double\"},{\"name\":\"b\",\"type\":\"string\"},{\"name\":\"z\",\"type\":\"int\",\"default\":7}]}")
        t = Avro.Table(buf(); reader_schema=rdr)
        @test Tables.columnnames(t) == [:a, :b, :z]
        @test Tables.getcolumn(t, :a) isa Vector{Float64} && Tables.getcolumn(t, :a)[3] == 3.0
        @test Tables.getcolumn(t, :z) == fill(Int32(7), 250)
        @test Avro.writerschema(t) !== Avro.schema(t)
        r = Avro.Rows(buf(); reader_schema=rdr)
        row1, _ = iterate(r)
        @test row1.z === Int32(7) && row1.a === 1.0
        close(r)
        t2 = Avro.Table(buf(); reader_schema=rdr, select=(:z, :b))
        @test Tables.columnnames(t2) == [:z, :b] && Tables.getcolumn(t2, :b)[1] == "r1" && Tables.getcolumn(t2, :z)[1] === Int32(7)
    end
    @testset "retained schemas on write; admission boundaries; ownership" begin
        io = IOBuffer()
        rsrc = Avro.Rows(buf())
        Avro.write(io, rsrc)
        close(rsrc)
        seekstart(io)
        t2 = Avro.Table(io)
        @test Avro.schema(t2).fields[4].schema isa Avro.EnumSchema             # the named enum survives Rows → write
        @test isequal(Tables.getcolumn(t2, :c), Tables.getcolumn(Avro.Table(buf()), :c))
        arrio = IOBuffer()
        w = Avro.Writer(arrio, P("{\"type\":\"array\",\"items\":\"long\"}"))
        push!(w, [1, 2])
        push!(w, Int64[])
        close(w)
        seekstart(arrio)
        rn = Avro.Rows(arrio)
        out = IOBuffer()
        Avro.write(out, rn)                                                     # non-record rows write datum-wise under the retained schema
        close(rn)
        seekstart(out)
        @test isequal(Avro.Reader(rr -> collect(Avro.eachdatum(rr)), out), Any[[1, 2], Int64[]])
        adm = Avro.SymbolAdmission(max_names=3, max_bytes=100)
        @test_throws Avro.LimitError Avro.Table(buf(); names=adm)
        t3 = Avro.Table(buf(); names=Avro.SymbolAdmission(max_names=100, max_bytes=1000))
        @test Tables.columnnames(t3) == [:a, :b, :c, :e]
        @test Tables.columnnames(Avro.Table(buf(); names=:trusted)) == [:a, :b, :c, :e]
        adm3 = Avro.SymbolAdmission(max_names=2, max_bytes=100)
        r3 = Avro.Rows(buf(); names=adm3)
        v3, _ = iterate(r3)
        @test Avro.record(v3).a === Int64(1)                                    # iteration interned nothing
        @test_throws Avro.LimitError Tables.schema(r3)                          # names admit only when requested
        close(r3)
        src = buf()
        mark0 = position(src)
        Avro.Table(src)
        @test isopen(src) && position(src) == mark0                             # the caller's IO is untouched
    end
    @testset "resolved little-endian decimals through tables (legacy fixture)" begin
        leg = joinpath(@__DIR__, "fixtures", "generated", "legacy1x", "avrojl112-null.avro")
        rdr = P("{\"type\":\"record\",\"name\":\"Record_5380612083211919099\",\"fields\":[{\"name\":\"a\",\"type\":\"double\"},{\"name\":\"dec\",\"type\":{\"type\":\"fixed\",\"name\":\"_avrojl1_fixed_7\",\"size\":16,\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}},{\"name\":\"z\",\"type\":\"int\",\"default\":7}]}")
        t = @test_logs (:warn, r"legacy") match_mode=:any Avro.Table(leg; reader_schema=rdr, legacy=:avrojl1, decimal_byteorder=:little)
        @test Tables.columnnames(t) == [:a, :dec, :z]
        @test Tables.getcolumn(t, :a) == [1.0, 2.0]                              # PromotePlan under the little rewrite
        @test Tables.getcolumn(t, :dec) == [Avro.Decimal(12345, 2), Avro.Decimal(-123, 2)]
        @test Tables.getcolumn(t, :z) == Int32[7, 7]
        rl = Avro.Rows(leg; reader_schema=rdr, legacy=:avrojl1, decimal_byteorder=:little, select=(:dec,))
        got = @test_logs (:warn, r"legacy") match_mode=:any [Tables.getcolumn(row, :dec) for row in rl]
        close(rl)
        @test got == [Avro.Decimal(12345, 2), Avro.Decimal(-123, 2)]
    end
    @testset "zero rows, zero columns, empty records" begin
        e0 = Avro.tobuffer(rows[1:0]; schema=s)
        t0 = Avro.Table(e0)
        @test length(t0) == 0 && Tables.columnnames(t0) == [:a, :b, :c, :e] && isempty(Tables.getcolumn(t0, :a))
        er = P("{\"type\":\"record\",\"name\":\"E\",\"fields\":[]}")
        io = IOBuffer()
        w = Avro.Writer(io, er)
        for _ in 1:5
            push!(w, (;))
        end
        close(w)
        seekstart(io)
        te = Avro.Table(io)
        @test length(te) == 5 && isempty(Tables.columnnames(te))
        t00 = Avro.Table(buf(); select=())
        @test length(t00) == 250 && isempty(Tables.columnnames(t00))
    end
end
