# Compile-cost gate (plan §4.5): after a warm-up covering every member of the closed value set E, decoding
# random heterogeneous schemas through the generic and column paths creates no new method instances for
# Avro's functions and bounded RSS growth.

"A schema whose generic representation is the value type `T`, or `nothing` when Avro cannot express it."
function schemafor(::Type{T}) where {T}
    leaves = Dict{Any,String}(
        Missing => "\"null\"", Bool => "\"boolean\"", Int32 => "\"int\"", Int64 => "\"long\"", Float32 => "\"float\"", Float64 => "\"double\"",
        Vector{UInt8} => "\"bytes\"", String => "\"string\"", Avro.Fixed => "{\"type\":\"fixed\",\"name\":\"Fx\",\"size\":2}",
        Avro.EnumValue => "{\"type\":\"enum\",\"name\":\"En\",\"symbols\":[\"a\",\"b\"]}",
        Avro.Decimal => "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":9,\"scale\":2}",
        Avro.WideDecimal => "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":40,\"scale\":2}",
        UUID => "{\"type\":\"string\",\"logicalType\":\"uuid\"}", Date => "{\"type\":\"int\",\"logicalType\":\"date\"}",
        Time => "{\"type\":\"long\",\"logicalType\":\"time-micros\"}",
        Avro.Timestamp{Millisecond} => "{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}",
        Avro.Timestamp{Microsecond} => "{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}",
        Avro.Timestamp{Nanosecond} => "{\"type\":\"long\",\"logicalType\":\"timestamp-nanos\"}",
        Avro.LocalTimestamp{Millisecond} => "{\"type\":\"long\",\"logicalType\":\"local-timestamp-millis\"}",
        Avro.LocalTimestamp{Microsecond} => "{\"type\":\"long\",\"logicalType\":\"local-timestamp-micros\"}",
        Avro.LocalTimestamp{Nanosecond} => "{\"type\":\"long\",\"logicalType\":\"local-timestamp-nanos\"}",
        Avro.Duration => "{\"type\":\"fixed\",\"name\":\"Du\",\"size\":12,\"logicalType\":\"duration\"}",
        Avro.Record => "{\"type\":\"record\",\"name\":\"Rec\",\"fields\":[{\"name\":\"f\",\"type\":\"int\"}]}",
        Avro.UnionValue => "[\"int\",\"string\"]")
    haskey(leaves, T) && return leaves[T]
    if T isa Union && Missing <: T
        inner = Base.nonmissingtype(T)
        inner === Avro.UnionValue && return nothing                          # unions cannot nest
        s = schemafor(inner)
        s === nothing && return nothing
        return "[\"null\",$s]"
    end
    T === Vector{Any} && return "{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":\"int\"}}"
    T === Avro.Map{Any} && return "{\"type\":\"map\",\"values\":{\"type\":\"map\",\"values\":\"int\"}}"
    if T <: Vector
        s = schemafor(eltype(T))
        s === nothing && return nothing
        return "{\"type\":\"array\",\"items\":$s}"
    end
    if T <: Avro.Map
        s = schemafor(eltype(T).parameters[2])
        s === nothing && return nothing
        return "{\"type\":\"map\",\"values\":$s}"
    end
    return nothing
end

# A sample value of `s` in the generic model. `pick` makes unions deterministic for the warm-up: 0 takes
# the null branch (the last branch of a non-nullable union), 1 the other (first) branch; -1 draws from
# `rng`. Arrays and maps carry one element per pick so both choices are present in every container.
function samplevalue(s::Avro.Schema, rng, pick::Int=-1)
    s isa Avro.NullSchema && return missing
    s isa Avro.BooleanSchema && return true
    s isa Avro.IntSchema && return s.logical isa Avro.DateLogical ? Date(2020) : (s.logical isa Avro.TimeMillis ? Time(1) : Int32(1))
    if s isa Avro.LongSchema
        l = s.logical
        l isa Avro.TimeMicros && return Time(2)
        l === nothing && return Int64(2)
        return Avro.juliatype(s)(5)
    end
    s isa Avro.FloatSchema && return 1.0f0
    s isa Avro.DoubleSchema && return 1.0
    s isa Avro.BytesSchema && return s.logical isa Avro.DecimalLogical ? (s.logical.precision > 38 ? Avro.WideDecimal(big(1), 2) : Avro.Decimal(1, 2)) : UInt8[1]
    s isa Avro.StringSchema && return s.logical isa Avro.UUIDLogical ? UUID(1) : "s"
    if s isa Avro.FixedSchema
        s.logical isa Avro.DurationLogical && return Avro.Duration(UInt32(1), UInt32(1), UInt32(1))
        return Avro.Fixed(s, zeros(UInt8, s.size))
    end
    s isa Avro.EnumSchema && return Avro.EnumValue(s, 1)
    s isa Avro.ArraySchema && return Any[samplevalue(s.items, rng, p) for p in picks(pick)]
    s isa Avro.MapSchema && return Avro.Map([(k, samplevalue(s.values, rng, p)) for (k, p) in zip(("k", "j"), picks(pick))])
    if s isa Avro.UnionSchema
        n = length(s.branches)
        nb = Avro.nullablebranch(s)
        if nb != 0
            choice = pick < 0 ? rand(rng, 0:1) : pick
            return choice == 0 ? missing : samplevalue(s.branches[3 - nb], rng, pick)
        end
        i = pick < 0 ? rand(rng, 1:n) : (pick == 0 ? n : 1)
        return Avro.UnionValue(i, samplevalue(s.branches[i], rng, pick))
    end
    return Avro.Record(s, Any[samplevalue(f.schema, rng, pick) for f in s.fields])
end

picks(pick::Int) = pick < 0 ? (-1, -1) : (0, 1)

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
