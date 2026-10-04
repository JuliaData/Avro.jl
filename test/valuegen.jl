# Deterministic generators over the closed value set E (plan §4.6) shared by the compile-cost gate, the
# storage oracle and other property tests.

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

function picks(pick::Int)
    return pick < 0 ? (-1, -1) : (0, 1)
end
