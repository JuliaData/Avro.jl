# Latency gate: 16 MB worst-density legal inputs — max_values_per_byte values per byte under the work
# rule — decode or skip in ≤ 10 s single-threaded on every supported Julia version. The fixtures derive
# their density from the limit, so recalibration keeps them worst-case.

@testset "Latency gate (worst-density legal inputs)" begin
    P = Avro.parseschema
    L = Avro.Limits()
    function varint(n)
        return Avro.encode(P("\"long\""), n)
    end

    times = Pair{String,Float64}[]
    function gate(name, f)
        t = @elapsed r = f()
        push!(times, name => t)
        @test t <= 10
        return r
    end

    function skipall(s, bytes, limits=L)
        return Avro.withbudget(limits) do b
            Avro.addinput!(b, length(bytes))
            d = Avro.Decoder(bytes, b)
            Avro.skip(Avro.readplan(s), d)
            return (d.pos == length(bytes) + 1, b.values)
        end
    end
    # the densest legal record: one boolean and (max_values_per_byte - 2) nulls per input byte
    nnulls = L.max_values_per_byte - 2
    dense = P("{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"b\",\"type\":\"boolean\"}," *
              join(["{\"name\":\"n$i\",\"type\":\"null\"}" for i in 1:nnulls], ",") * "]}}")
    N = 16_000_000
    small = Avro.Limits(max_total_bytes=256 << 20)
    bytes = vcat(varint(N), fill(0x01, N), UInt8[0x00])
    @test gate("skip $(N) dense records ($(N * L.max_values_per_byte) values)", () -> skipall(dense, bytes)) == (true, N * L.max_values_per_byte + 1)
    @test_throws Avro.LimitError Avro.decode(dense, bytes; limits=small)        # a lowered ceiling trips first
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
    M = N ÷ 16
    deepbytes = vcat(varint(M), fill(0x01, 16 * M), UInt8[0x00])
    @test gate("skip $(M) depth-80 records ($(97M + 1) values)", () -> skipall(deep, deepbytes)) == (true, 97 * M + 1)
    capped = Avro.Limits(max_total_values=97 * M + 1)
    @test skipall(deep, deepbytes, capped) == (true, 97 * M + 1)
    @test_throws Avro.LimitError skipall(deep, vcat(varint(M + 1), fill(0x01, 16 * (M + 1)), UInt8[0x00]), capped)   # one datum over max_total_values
    # nested empty arrays: one value and one 40-byte shell per byte
    empties = P("{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":\"int\"}}")
    emptybytes = vcat(varint(N), fill(0x00, N), UInt8[0x00])
    @test gate("skip $(N) empty arrays", () -> skipall(empties, emptybytes)) == (true, N + 1)
    @test_throws Avro.LimitError Avro.decode(empties, emptybytes; limits=small)
    # wide JSON object whose keys share a 1 KB prefix (the duplicate-key sort compares long prefixes)
    ms = P("{\"type\":\"map\",\"values\":\"long\"}")
    json = "{" * join(["\"" * "p"^1000 * string(i; pad=6) * "\":1" for i in 1:60_000], ",") * "}"
    @test length(gate("fromjson 60K keys × 1 KB common prefix (60 MB)", () -> Avro.fromjson(ms, json))) == 60_000
    # binary map of 600K keys sharing a 90-byte prefix in reverse order (sorted-permutation construction)
    keys = ["q"^90 * string(i; pad=7) for i in 600_000:-1:1]
    mapbytes = vcat(varint(length(keys)), [vcat(Avro.encode(P("\"string\""), k), UInt8[0x02]) for k in keys]..., UInt8[0x00])
    @test length(gate("decode 600K-key map, 90-byte common prefix, reversed", () -> Avro.decode(ms, mapbytes))) == 600_000
    @info "latency gate (seconds)" times
end
