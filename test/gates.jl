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

"The full signature set, so a gate failure names the leaked specializations."
function avrospecset()
    out = Set{Any}()
    for name in names(Avro; all=true)
        isdefined(Avro, name) || continue
        f = getfield(Avro, name)
        f isa Function || continue
        for m in methods(f)
            m.module === Avro || continue
            for sp in Base.specializations(m)
                push!(out, sp.specTypes)
            end
        end
    end
    return out
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
    # One barrier shared by the warm-up and the measured batch: every harness call context (abstract
    # `Avro.Schema` arguments, the column closure) compiles here exactly once during the warm-up, so
    # the measured batch reports only schema-content-driven method instances (review round 2).
    @noinline function runone(s::Avro.Schema, @nospecialize(v))
        bytes = Avro.encode(s, v)
        Avro.decode(s, bytes)
        plan = Avro.readplan(s)
        if plan isa Avro.RecordPlan
            Avro.withbudget(Avro.Limits()) do budget
                Avro.addinput!(budget, length(bytes))
                d = Avro.Decoder(bytes, budget)
                cols = Avro.columnbuilders(plan, nothing, 1, budget)
                Avro.decoderow!(cols, d)
                foreach(c -> c isa Avro.TypedColumn && Avro.finishcolumn!(c, budget), cols)
            end
        end
        Avro.fromjson(s, Avro.tojson(s, v))
        return nothing
    end

    @noinline function warmentries(s::Avro.Schema, @nospecialize(v))
        dw = Avro.DatumWriter(s)
        Avro.DatumReader(s)(dw(v))
        Avro.json(s)
        Avro.canonical(s)
        Avro.fingerprint(s)
        return nothing
    end

    function exercise(n)
        completed = 0
        while completed < n
            src = "{\"type\":\"record\",\"name\":\"Top\",\"fields\":[" *
                  join(["{\"name\":\"g$i\",\"type\":$(randomschema(1))}" for i in 1:rand(rng, 0:8)], ",") * "]}"
            s = try
                Avro.parseschema(src)
            catch e
                e isa Avro.SchemaError && continue      # a random draw reused a named type with a different definition
                rethrow()
            end
            runone(s, samplevalue(s, rng))
            completed += 1
        end
        return completed
    end
    # the barrier and every closed-kind entry point compile during the warm-up phase
    for T in E
        src = schemafor(T)
        src === nothing && continue
        sw = Avro.parseschema(src)
        vw = samplevalue(sw, rng)
        runone(sw, vw)
        warmentries(sw, vw)
    end
    # every closed composition context, deterministically: the measured batch draws containers whose
    # elements narrow to members of E (narrowelement), so warming each (container kind, E element)
    # pair covers the runtime dispatch and inference contexts the random batch would otherwise
    # first-encounter inside the measurement. A separate seed leaves the measured stream untouched.
    let wrng = Random.Xoshiro(11)
        for (wi, leafsrc) in enumerate(leaves)
            for shape in ("{\"type\":\"array\",\"items\":" * leafsrc * "}",
                          "{\"type\":\"map\",\"values\":" * leafsrc * "}",
                          "{\"type\":\"record\",\"name\":\"WC$(wi)\",\"fields\":[{\"name\":\"f\",\"type\":" * leafsrc * "}]}")
                sw = Avro.parseschema(shape)
                runone(sw, samplevalue(sw, wrng))
            end
        end
    end
    for extra in ("{\"type\":\"array\",\"items\":[\"int\",\"string\",\"boolean\"]}",
                  "{\"type\":\"map\",\"values\":[\"long\",\"null\",\"double\"]}",
                  "{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":[\"null\",\"bytes\"]}}",
                  "{\"type\":\"array\",\"items\":{\"type\":\"map\",\"values\":\"long\"}}",
                  "{\"type\":\"array\",\"items\":[\"int\",{\"type\":\"array\",\"items\":\"long\"}]}",
                  "{\"type\":\"array\",\"items\":[\"null\",{\"type\":\"array\",\"items\":\"long\"}]}",
                  "{\"type\":\"array\",\"items\":[\"null\",{\"type\":\"map\",\"values\":\"long\"}]}",
                  "{\"type\":\"record\",\"name\":\"WTop\",\"fields\":[{\"name\":\"g\",\"type\":{\"type\":\"map\",\"values\":[\"boolean\",\"string\"]}}]}")
        se = Avro.parseschema(extra)
        for pick in (0, 1)
            runone(se, samplevalue(se, rng, pick))
        end
    end
    let s0 = Avro.parseschema("{\"type\":\"record\",\"name\":\"W0\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]}")
        store = Avro.SchemaCache()
        Avro.register!(store, s0)
        Avro.decodesingle(Avro.encodesingle(s0, (a=Int64(1),)), store)
        io0 = IOBuffer()
        w0 = Avro.Writer(io0, s0)
        push!(w0, (a=Int64(1),))
        close(w0)
        seekstart(io0)
        Tables.columntable(Avro.Table(io0))
    end
    exercise(2)     # the agreed warm-up: one pass over the closed schema/value kinds
    before = avrospecializations()
    # The agreed §10.2 protocol (review round 1, R14): the RSS baseline is taken after the `E` warm-up,
    # and the single post-warm-up batch of 1,000 random schemas must grow the high-water mark by less
    # than 50 MB. Full collections on both sides keep the collector's heap-sizing policy out of the
    # delta; the measured number is compiled code plus retained caches.
    specs0 = avrospecset()
    GC.gc(true)
    GC.gc(true)
    rss0 = Sys.maxrss()
    completed = 0
    for _ in 1:200                                     # small fully-collected slices so the high-water
        completed += exercise(5)                       # delta measures retention, not transient peaks
        GC.gc(true)
    end
    @test completed == 1000
    after = avrospecializations()
    if after != before
        for t in setdiff(avrospecset(), specs0)
            println("  leaked specialization: ", t)
        end
    end
    @test after == before
    GC.gc(true)
    growth = (Sys.maxrss() - rss0) / 2^20
    @info "compile-cost gate" specializations=before new=after - before rss_growth_mb=round(growth; digits=1)
    @test growth < 50
end
