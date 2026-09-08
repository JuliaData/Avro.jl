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
    function decodeall(nrows; selected=nothing, capacity=nrows, limits=Avro.Limits(), bytes=bytes)
        return Avro.withbudget(limits) do budget
            Avro.addinput!(budget, length(bytes))
            d = Avro.Decoder(bytes, budget)
            cols = Avro.columnbuilders(plan, selected, capacity, budget)
            for _ in 1:nrows
                Avro.countvalues!(budget)
                Avro.decoderow!(cols, d)
            end
            d.pos == length(bytes) + 1 || error("not consumed")
            return [c isa Avro.TypedColumn ? Avro.finishcolumn!(c, budget) : nothing for c in cols]
        end
    end
    cols = decodeall(50)
    @test cols[1] == 1:50 && cols[1] isa Vector{Int64}
    @test cols[2] == [i / 2 for i in 1:50] && cols[2] isa Vector{Float64}
    @test cols[3] == ["n$i" for i in 1:50] && cols[3] isa Vector{String}
    @test isequal(cols[4], [i % 3 == 0 ? missing : isodd(i) for i in 1:50]) && cols[4] isa Vector{Union{Missing,Bool}}
    @test cols[5] == [["t$i"] for i in 1:50] && cols[5] isa Vector{Vector{String}}
    @test cols[6][7] == Avro.Map{Vector{Int32}}([("k", Int32[7])]) && cols[6] isa Vector{Avro.Map{Any}} || cols[6] isa Vector{Avro.Map{Vector{Int32}}}
    @test cols[6] isa Vector{Avro.Map{Any}}                                   # one level of typed nesting: Map{Any} for nested arrays
    @test cols[7] == [Avro.EnumValue(s.fields[7].schema, isodd(i) ? 1 : 2) for i in 1:50] && cols[7] isa Vector{Avro.EnumValue}
    @test all(eltype(c) in Avro.valuetypes() for c in cols)
    # projection: unselected fields are skipped, growth beyond the capacity hint, exact trimming
    cols2 = decodeall(50; selected=[1, 3], capacity=4)
    @test cols2[1] == 1:50 && cols2[3] == ["n$i" for i in 1:50] && cols2[2] === nothing && cols2[4] === nothing
    @test length(cols2[1]) == 50
    cols3 = decodeall(50; capacity=100)
    @test length(cols3[1]) == 50 && length(cols3[5]) == 50
    @test_throws ArgumentError Avro.columnbuilders(plan, nothing, -1, Avro.Budget(Avro.Limits(); available=1 << 30))
    # schema-width-dependent builder and fused-skip state is part of the guarded operation
    nwide = 1000
    widefields = join(("{\"name\":\"f$i\",\"type\":\"null\"}" for i in 1:nwide), ",")
    wideschema = P("{\"type\":\"record\",\"name\":\"WideNull\",\"fields\":[" * widefields * "]}")
    widebudget = Avro.Budget(Avro.Limits(); available=1 << 40)
    wideplan = Avro.readplan(wideschema; budget=widebudget)
    wideprojection = Avro.positionalprojection(Int[], nwide, widebudget)
    beforewide = widebudget.reserved
    widecols = Avro.columnbuilders(wideplan, wideprojection, 1, widebudget)
    @test widebudget.reserved - beforewide == Avro.columnbuildersstate(widecols)
    beforefuse = widebudget.reserved
    widecells = Avro.fuseskips(widecols, widebudget)
    @test widebudget.reserved - beforefuse == Avro.fusedstate(widecells, widecols)
    Avro.releasefused!(widecells, widecols, widebudget)
    Avro.releasecolumnbuilders!(widecols, widebudget)
    @test widebudget.reserved == beforewide
    # storage is charged exactly at allocation
    budget = Avro.Budget(Avro.Limits(); available=1 << 40)
    before = budget.reserved
    c = Avro.makecolumn(Union{Missing,Int32}, Avro.IntPlan(), 1000, budget)
    @test budget.reserved - before == 40 + 1000 * (4 + 1) +
          Avro.columnnodebytes(typeof(c))                                  # tag byte per isbits-union element plus builder shell
    before = budget.reserved
    stringcolumn = Avro.makecolumn(String, Avro.StringPlan(), 1000, budget)
    @test budget.reserved - before == 40 + 1000 * 8 +
          Avro.columnnodebytes(typeof(stringcolumn))
    before = budget.reserved
    floatcolumn = Avro.makecolumn(Float64, Avro.DoublePlan(), 10, budget)
    @test budget.reserved - before == 40 + 80 +
          Avro.columnnodebytes(typeof(floatcolumn))
    @test_throws Avro.LimitError Avro.makecolumn(Float64, Avro.DoublePlan(), 1 << 30, Avro.Budget(Avro.Limits(); available=1 << 40))
    # malformed cells surface as DataErrors; skipped cells are validated under :strict
    bad = copy(bytes); bad[end] = 0x09                                         # enum index zigzag 9 → -5: out of range
    @test_throws Avro.DataError decodeall(50; bytes=bad)
    @test_throws Avro.DataError decodeall(50; selected=[1], bytes=bad)
    # decodecolumns convenience
    dc = Avro.withbudget(Avro.Limits()) do budget
        Avro.addinput!(budget, length(bytes))
        d = Avro.Decoder(bytes, budget)
        cs = Avro.decodecolumns(plan, d, 50; selected=[2])
        Avro.finishcolumn!(cs[2], budget)
    end
    @test dc == [i / 2 for i in 1:50]
    # one loop, dynamic dispatch per cell: the kernel allocates only the values (isbits columns: zero)
    bs = P("{\"type\":\"record\",\"name\":\"B\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"double\"}]}")
    bb = reduce(vcat, [Avro.encode(bs, (a=i, b=1.0)) for i in 1:100])
    bp = Avro.readplan(bs)
    function kernel(cols, d, n)
        d.pos = 1
        for c in cols
            c.len = 0
        end
        for _ in 1:n
            Avro.decoderow!(cols, d)
        end
        return nothing
    end
    budget = Avro.Budget(Avro.Limits(); available=1 << 40)
    Avro.addinput!(budget, 1 << 20)
    d = Avro.Decoder(bb, budget)
    cols = Avro.columnbuilders(bp, nothing, 100, budget)
    kernel(cols, d, 100)
    @test @allocated(kernel(cols, d, 100)) == 0
    # a string column allocates exactly one String per cell (plan §9.12)
    ss = P("{\"type\":\"record\",\"name\":\"S\",\"fields\":[{\"name\":\"s\",\"type\":\"string\"}]}")
    sb = reduce(vcat, [Avro.encode(ss, (s="row$i",)) for i in 1:200])
    scols = Avro.columnbuilders(Avro.readplan(ss), nothing, 200, budget)
    sd = Avro.Decoder(sb, budget)
    kernel(scols, sd, 200)
    a100 = @allocations(kernel(scols, sd, 100))
    a200 = @allocations(kernel(scols, sd, 200))
    @test a200 - a100 == 100 && a100 <= 101                                 # one String per cell (at most one per call besides)
end
