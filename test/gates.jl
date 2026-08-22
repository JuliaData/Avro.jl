# Compile-cost gate (plan §4.5): after a warm-up covering every member of the closed value set E, decoding
# random heterogeneous schemas through the generic and column paths creates no new method instances for
# Avro's functions and bounded RSS growth. Value generators: test/valuegen.jl.

function avrospecializations()
    n = 0
    for name in names(Avro; all=true)
        isdefined(Avro, name) || continue
        f = getfield(Avro, name)
        f isa Function || continue
        for m in methods(f)
            m.module === Avro || continue
            n += length(Base.specializations(m))
        end
    end
    return n
end

@testset "Value set E and the compile-cost gate" begin
    E = Avro.valuetypes()
    @test length(E) == length(unique(E)) && Avro.Record in E && Vector{Any} in E && Union{Missing,Avro.Map{Int32}} in E
    rng = Random.Xoshiro(7)
    # warm-up: one schema per expressible member of E, both union choices at every level, through the
    # generic, column and JSON paths
    warmed = 0
    for T in E
        src = schemafor(T)
        src === nothing && continue
        s = Avro.parseschema(src)
        @test Avro.juliatype(s) === T
        rec = Avro.parseschema("{\"type\":\"record\",\"name\":\"Row\",\"fields\":[{\"name\":\"c\",\"type\":$src}]}")
        plan = Avro.readplan(rec)
        for pick in (0, 1)
            v = samplevalue(s, rng, pick)
            bytes = Avro.encode(s, v)
            @test typeof(Avro.decode(s, bytes)) <: T
            rb = Avro.encode(rec, (c=v,))
            Avro.withbudget(Avro.Limits()) do budget
                Avro.addinput!(budget, length(rb))
                d = Avro.Decoder(rb, budget)
                cols = Avro.columnbuilders(plan, nothing, 1, budget)
                Avro.decoderow!(cols, d)
                col = Avro.finishcolumn!(cols[1], budget)
                @test eltype(col) === T && length(col) == 1
            end
            Avro.fromjson(s, Avro.tojson(s, v))
        end
        warmed += 1
    end
    @test warmed > 200
    # random heterogeneous schemas (widths, orders, nesting) drawn from the warmed members
    leaves = [s for s in (schemafor(T) for T in E) if s !== nothing]
    function randomschema(depth)
        k = rand(rng, 1:(depth > 2 ? 1 : 4))
        k == 1 && return rand(rng, leaves)
        k == 2 && return "{\"type\":\"array\",\"items\":$(randomschema(depth + 1))}"
        k == 3 && return "{\"type\":\"map\",\"values\":$(randomschema(depth + 1))}"
        n = rand(rng, 0:12)
        return "{\"type\":\"record\",\"name\":\"R$(depth)_$(rand(rng, 1:1000000))\",\"fields\":[" *
               join(["{\"name\":\"f$i\",\"type\":$(randomschema(depth + 1))}" for i in 1:n], ",") * "]}"
    end
    function exercise(n)
        for _ in 1:n
            src = "{\"type\":\"record\",\"name\":\"Top\",\"fields\":[" *
                  join(["{\"name\":\"g$i\",\"type\":$(randomschema(1))}" for i in 1:rand(rng, 0:8)], ",") * "]}"
            s = try
                Avro.parseschema(src)
            catch e
                e isa Avro.SchemaError && continue      # a random draw reused a named type with a different definition
                rethrow()
            end
            v = samplevalue(s, rng)
            bytes = Avro.encode(s, v)
            Avro.decode(s, bytes)
            Avro.withbudget(Avro.Limits()) do budget
                Avro.addinput!(budget, length(bytes))
                d = Avro.Decoder(bytes, budget)
                cols = Avro.columnbuilders(Avro.readplan(s), nothing, 1, budget)
                Avro.decoderow!(cols, d)
                foreach(c -> c isa Avro.TypedColumn && Avro.finishcolumn!(c, budget), cols)
            end
            Avro.fromjson(s, Avro.tojson(s, v))
        end
    end
    exercise(50)                                       # second warm-up: random shapes once
    GC.gc()
    before = avrospecializations()
    exercise(1000)
    after = avrospecializations()
    @test after == before
    # `Sys.maxrss` is a high-water mark, so the first thousand schemas also bring the heap to its
    # steady-state peak; the delta over a second thousand is retained growth (compiled code, caches), not
    # the collector's heap-sizing policy.
    rss0 = Sys.maxrss()
    exercise(1000)
    growth = (Sys.maxrss() - rss0) / 2^20
    @test avrospecializations() == before
    @info "compile-cost gate" specializations=before new=after - before rss_growth_mb=round(growth; digits=1)
    @test growth < 50
end
