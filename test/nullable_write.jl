@enum NullableWriteCode nullable_alpha nullable_beta

@testset "Nullable writes preserve validation and avoid per-value allocation" begin
    P = Avro.parseschema
    failure(f) = try
        f()
        nothing
    catch err
        err
    end

    @testset "both branch orders and explicit choices" begin
        for src in ("[\"null\",\"long\"]", "[\"long\",\"null\"]")
            s = P(src)
            null = Avro.nullablebranch(s)
            value = 3 - null
            writer = Avro.DatumWriter(s)
            for x in (missing, nothing, Int64(1234), Avro.UnionValue(value, Int64(1234)),
                      Avro.UnionValue(null, missing))
                expected = x isa Avro.UnionValue ? x.value : x
                expected === nothing && (expected = missing)
                @test isequal(Avro.decode(s, writer(x)), expected)
            end
            @test_throws Avro.EncodeError writer(Avro.UnionValue(null, Int64(1)))
            @test_throws Avro.EncodeError writer(Avro.UnionValue(value, missing))
            @test_throws Avro.EncodeError writer(Avro.UnionValue(3, Int64(1)))
            @test_throws Avro.EncodeError writer("not an integer")
            @test Avro.decode(s, writer(Int64(1234))) == 1234
        end
        enum = P("""["null",{"type":"enum","name":"NullableWriteCode",
                             "symbols":["nullable_alpha","nullable_beta"]}]""")
        @test String(Avro.decode(enum, Avro.encode(enum, nullable_beta))) == "nullable_beta"
    end

    @testset "nested diagnostics and reusable prepared writers" begin
        s = P("""{"type":"record","name":"NullablePaths","fields":[
            {"name":"xs","type":{"type":"array","items":["null","int"]}}]}""")
        writer = Avro.DatumWriter(s)
        err = failure(() -> writer((xs=Any[missing, "bad"],)))
        @test err isa Avro.EncodeError
        @test err.path == "\$.xs[1]"
        @test err.schema === s.fields[1].schema.items.branches[2]
        @test failure(() -> writer((xs=Any["bad"],))).path == "\$.xs[0]"      # no stale path from the last failure
        @test isequal(Avro.decode(s, writer((xs=Any[missing, Int32(1234)],))).xs,
                      Union{Missing,Int32}[missing, 1234])

        nullable_record = P("""["null",{"type":"record","name":"NullableRecord",
            "fields":[{"name":"value","type":"int"}]}]""")
        writer = Avro.DatumWriter(nullable_record)
        err = failure(() -> writer((value="bad",)))
        @test err isa Avro.EncodeError && err.path == "\$.value"
        @test err.schema === nullable_record.branches[2].fields[1].schema
        @test Avro.decode(nullable_record, writer((value=Int32(1234),))).value == 1234
    end

    @testset "depth limit; a prepared writer recovers" begin
        s = P("[\"null\",\"long\"]")
        for x in (missing, Int64(1234))
            @test isequal(Avro.decode(s, Avro.encode(s, x)), x)
        end
        s = P("""{"type":"record","name":"NullableDepth","fields":[
            {"name":"next","type":["null","NullableDepth"]}]}""")
        writer = Avro.DatumWriter(s)
        @test isequal(Avro.decode(s, writer((next=missing,))).next, missing)
        deep = Avro.Record(s, Any[missing])
        for _ in 1:1100
            deep = Avro.Record(s, Any[deep])
        end
        err = failure(() -> writer(deep))
        @test err isa Avro.LimitError && err.limit === :max_depth && err.value == 1024
        @test isequal(Avro.decode(s, writer((next=missing,))).next, missing)
    end

    @testset "table allocations stay independent of nullable row count" begin
        table(n) = (x=Union{Missing,Int64}[isodd(i) ? missing : Int64(i + 1024) for i in 1:n],
                    y=Union{Missing,Float64}[isodd(i) ? Float64(i) : missing for i in 1:n])
        small, large = table(2000), table(4000)
        Avro.tobuffer(small)
        Avro.tobuffer(large)
        @test isequal(Tables.columntable(Avro.Table(Avro.tobuffer(large))), large)
        @test @allocations(Avro.tobuffer(large)) - @allocations(Avro.tobuffer(small)) < 1000
    end
end
