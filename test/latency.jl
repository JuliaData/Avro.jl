# Latency gate (plan §4.4, provisional in Phase 2): the worst-density legal inputs the default limits admit
# — 16 values per byte under the work rule, up to max_total_values — decode or skip in ≤ 10 s
# single-threaded on every supported Julia version. The measured times are recorded in STATUS.md; the
# work constants stay provisional until the Phase 4a container shapes are added.

@testset "Latency gate (worst-density legal inputs)" begin
    P = Avro.parseschema
    L = Avro.Limits()
    varint(n) = Avro.encode(P("\"long\""), n)
    times = Pair{String,Float64}[]
    function gate(name, f)
        t = @elapsed r = f()
        push!(times, name => t)
        @test t <= 10
        return r
    end
    skipall(s, bytes) = Avro.withbudget(L) do b
        Avro.addinput!(b, length(bytes))
        d = Avro.Decoder(bytes, b)
        Avro.skip(Avro.readplan(s), d)
        (d.pos == length(bytes) + 1, b.values)
    end
    # 16 values per byte: records of one boolean and 14 nulls
    dense = P("{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"b\",\"type\":\"boolean\"}," *
              join(["{\"name\":\"n$i\",\"type\":\"null\"}" for i in 1:14], ",") * "]}}")
    N = 16_000_000
    bytes = vcat(varint(N), fill(0x01, N), UInt8[0x00])
    @test gate("skip 16M dense records (256M values)", () -> skipall(dense, bytes)) == (true, 256_000_001)
    @test_throws Avro.LimitError Avro.decode(dense, bytes)                       # the generic decode trips the ceiling first
    rows = fill(0x01, N)
    nb = gate("column decode of 16M dense rows (one Bool column)", () -> Avro.withbudget(L) do b
        Avro.addinput!(b, length(rows))
        d = Avro.Decoder(rows, b)
        cols = Avro.decodecolumns(Avro.readplan(dense.items), d, N; selected=[1])
        length(Avro.finishcolumn!(cols[1], b))
    end)
    @test nb == N
    # deeply nested records (the schema depth limit allows ~80 levels) with a 16-boolean leaf: 97 values per 16 bytes
    nested = "{\"type\":\"record\",\"name\":\"L\",\"fields\":[" * join(["{\"name\":\"b$i\",\"type\":\"boolean\"}" for i in 1:16], ",") * "]}"
    for k in 1:80
        nested = "{\"type\":\"record\",\"name\":\"R$k\",\"fields\":[{\"name\":\"f\",\"type\":$nested}]}"
    end
    deep = P("{\"type\":\"array\",\"items\":$nested}")
    M = (L.max_total_values - 2) ÷ 97
    deepbytes = vcat(varint(M), fill(0x01, 16 * M), UInt8[0x00])
    @test gate("skip $(M) depth-80 records ($(97M + 1) values)", () -> skipall(deep, deepbytes)) == (true, 97 * M + 1)
    @test_throws Avro.LimitError skipall(deep, vcat(varint(M + 1), fill(0x01, 16 * (M + 1)), UInt8[0x00]))   # one datum over max_total_values
    # nested empty arrays: one value and one 40-byte shell per byte
    empties = P("{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":\"int\"}}")
    emptybytes = vcat(varint(N), fill(0x00, N), UInt8[0x00])
    @test gate("skip 16M empty arrays", () -> skipall(empties, emptybytes)) == (true, N + 1)
    @test_throws Avro.LimitError Avro.decode(empties, emptybytes)
    # wide JSON object whose keys share a 1 KB prefix (the comparison rule)
    ms = P("{\"type\":\"map\",\"values\":\"long\"}")
    json = "{" * join(["\"" * "p"^1000 * string(i; pad=6) * "\":1" for i in 1:60_000], ",") * "}"
    @test length(gate("fromjson 60K keys × 1 KB common prefix (60 MB)", () -> Avro.fromjson(ms, json))) == 60_000
    # binary map of 600K keys sharing a 90-byte prefix in reverse order (sorted-permutation construction)
    keys = ["q"^90 * string(i; pad=7) for i in 600_000:-1:1]
    mapbytes = vcat(varint(length(keys)), [vcat(Avro.encode(P("\"string\""), k), UInt8[0x02]) for k in keys]..., UInt8[0x00])
    @test length(gate("decode 600K-key map, 90-byte common prefix, reversed", () -> Avro.decode(ms, mapbytes))) == 600_000
    @info "latency gate (seconds)" times
end
