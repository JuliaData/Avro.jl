@testset "Column builders" begin
    P = Avro.parseschema
    s = P("""{"type":"record","name":"R","fields":[
        {"name":"id","type":"long"},{"name":"x","type":"double"},{"name":"name","type":"string"},
        {"name":"flag","type":["null","boolean"]},{"name":"tags","type":{"type":"array","items":"string"}},
        {"name":"m","type":{"type":"map","values":{"type":"array","items":"int"}}},
        {"name":"e","type":{"type":"enum","name":"E","symbols":["a","b"]}}]}""")
    rows = [(id=i, x=i / 2, name="n$i", flag=(i % 3 == 0 ? missing : isodd(i)), tags=["t$i"], m=Dict("k" => [i]), e=(isodd(i) ? "a" : "b")) for i in 1:50]
    bytes = reduce(vcat, [Avro.encode(s, r) for r in rows])
    plan = Avro.readplan(s)
    function keeping(selected, nfields)
        slots = zeros(Int, nfields)
        slots[selected] .= eachindex(selected)
        return Avro.Projection(selected, slots)
    end
    function decodeall(nrows; selected=nothing, capacity=nrows, limits=Avro.Limits(), bytes=bytes)
        d = Avro.Decoder(bytes, limits)
        projection = selected === nothing ? nothing : keeping(selected, length(s.fields))
        cols = Avro.columnbuilders(plan, projection, capacity)
        for _ in 1:nrows
            Avro.decoderow!(cols, d)
        end
        d.pos == length(bytes) + 1 || error("not consumed")
        return [c isa Avro.TypedColumn ? Avro.finishcolumn!(c) : nothing for c in cols]
    end
    cols = decodeall(50)
    @test cols[1] == 1:50 && cols[1] isa Vector{Int64}
    @test cols[2] == [i / 2 for i in 1:50] && cols[2] isa Vector{Float64}
    @test cols[3] == ["n$i" for i in 1:50] && cols[3] isa Avro.DataStrings.StringVector{Avro.DataString}
    @test isequal(cols[4], [i % 3 == 0 ? missing : isodd(i) for i in 1:50]) && cols[4] isa Vector{Union{Missing,Bool}}
    @test cols[5] == [["t$i"] for i in 1:50] && cols[5] isa Vector{Vector{String}}
    @test cols[6][7] == Dict("k" => Int32[7])
    @test cols[6] isa Vector{Dict{String,Any}}                              # one level of typed nesting: Dict{String,Any} for nested arrays
    @test cols[7] == [Avro.EnumValue(s.fields[7].schema, isodd(i) ? 1 : 2) for i in 1:50] && cols[7] isa Vector{Avro.EnumValue}
    @test all(eltype(c) in Avro.valuetypes() || eltype(c) === Avro.DataString for c in cols)
    # projection: unselected fields are skipped, growth beyond the capacity hint, exact trimming
    cols2 = decodeall(50; selected=[1, 3], capacity=4)
    @test cols2[1] == 1:50 && cols2[3] == ["n$i" for i in 1:50] && cols2[2] === nothing && cols2[4] === nothing
    @test length(cols2[1]) == 50
    cols3 = decodeall(50; capacity=100)
    @test length(cols3[1]) == 50 && length(cols3[5]) == 50
    @test_throws ArgumentError Avro.columnbuilders(plan, nothing, -1)
    # unselected fields fuse into skip runs; a fused run still consumes the row and counts every value
    nwide = 1000
    widefields = join(("{\"name\":\"f$i\",\"type\":\"null\"}" for i in 1:nwide), ",")
    wideschema = P("{\"type\":\"record\",\"name\":\"WideNull\",\"fields\":[" * widefields * "]}")
    widecols = Avro.columnbuilders(Avro.readplan(wideschema), keeping(Int[], nwide), 1)
    widecells = Avro.fuseskips(widecols)
    @test length(widecells) < length(widecols)
    wided = Avro.Decoder(UInt8[])
    Avro.decoderow!(widecells, wided)
    @test wided.pos == 1 && wided.values == nwide + 1                       # the record and each skipped null
    # malformed cells surface as DataErrors; skipped cells are validated too
    bad = copy(bytes); bad[end] = 0x09                                         # enum index zigzag 9 → -5: out of range
    @test_throws Avro.DataError decodeall(50; bytes=bad)
    @test_throws Avro.DataError decodeall(50; selected=[1], bytes=bad)
    # one loop, dynamic dispatch per cell: the kernel allocates only the values (isbits columns: zero)
    bs = P("{\"type\":\"record\",\"name\":\"B\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"double\"}]}")
    bb = reduce(vcat, [Avro.encode(bs, (a=i, b=1.0)) for i in 1:100])
    bp = Avro.readplan(bs)
    function kernel(cols, d, n)
        Avro.reset!(d, d.buf, 1)
        for c in cols
            c.len = 0
        end
        for _ in 1:n
            Avro.decoderow!(cols, d)
        end
        return nothing
    end
    d = Avro.Decoder(bb)
    cols = Avro.columnbuilders(bp, nothing, 100)
    kernel(cols, d, 100)
    @test @allocated(kernel(cols, d, 100)) == 0
    # nullable leaf columns too: the union plan carries its non-null branch's concrete plan type
    ns = P("{\"type\":\"record\",\"name\":\"N\",\"fields\":[{\"name\":\"a\",\"type\":[\"null\",\"long\"]},{\"name\":\"b\",\"type\":[\"double\",\"null\"]}]}")
    nrows = [(a=isodd(i) ? missing : 1000 * i, b=i % 3 == 0 ? missing : i / 3) for i in 1:100]
    nb = reduce(vcat, [Avro.encode(ns, r) for r in nrows])
    ncols = Avro.columnbuilders(Avro.readplan(ns), nothing, 100)
    nd = Avro.Decoder(nb)
    kernel(ncols, nd, 100)
    @test @allocated(kernel(ncols, nd, 100)) == 0
    @test isequal(ncols[1].data, [r.a for r in nrows]) && isequal(ncols[2].data, [r.b for r in nrows])
    # a string column allocates nothing for a short string, and for long ones only a buffer as one fills
    ss = P("{\"type\":\"record\",\"name\":\"S\",\"fields\":[{\"name\":\"s\",\"type\":\"string\"}]}")
    sb = reduce(vcat, [Avro.encode(ss, (s="row$i",)) for i in 1:200])
    scols = Avro.columnbuilders(Avro.readplan(ss), nothing, 200)
    sd = Avro.Decoder(sb)
    kernel(scols, sd, 200)
    @test @allocations(kernel(scols, sd, 200)) == @allocations(kernel(scols, sd, 100)) <= 1
    lb = reduce(vcat, [Avro.encode(ss, (s="a longer row, number $i",)) for i in 1:200])
    lcols = Avro.columnbuilders(Avro.readplan(ss), nothing, 200)
    ld = Avro.Decoder(lb)
    kernel(lcols, ld, 200)
    @test @allocations(kernel(lcols, ld, 200)) < 10
    @test Avro.finishcolumn!(lcols[1]) == ["a longer row, number $i" for i in 1:200]
end
