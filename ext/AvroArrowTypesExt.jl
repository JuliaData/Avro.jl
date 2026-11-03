# An `Avro.Record` holds its field values untyped, so Arrow.jl cannot type a struct column from them. This extension
# lowers each record to a `NamedTuple` whose field types come from the record's schema, so a struct column has one
# Arrow type in every row whatever values the rows hold, and a decimal inside it has the schema's precision and
# scale. `Avro.Duration` lowers to the `Durations.Duration` that Arrow.jl writes as a month-day-nanosecond interval.
module AvroArrowTypesExt

using Avro: Avro
using DataDecimals: DataDecimals
using Durations: Durations
import ArrowTypes

function ArrowTypes.ArrowType(::Type{Avro.Record})
    return NamedTuple
end

# A record's storage type comes from its schema, which `Avro.Record` does not carry, so a column with no
# records has no typed placeholder and ArrowTypes writes it as nulls.
function ArrowTypes.default(::Type{Avro.Record})
    return missing
end

# ponytail: the struct type is rebuilt for every record; cache it per schema if record columns write too slowly.
function ArrowTypes.toarrow(r::Avro.Record)
    s = Avro.schema(r)
    T = storagetype(s, Avro.RecordSchema[])
    return T(Tuple(arrowvalue(f.schema, v) for (f, v) in zip(s.fields, getfield(r, :values))))
end

function ArrowTypes.ArrowType(::Type{Avro.Duration})
    return Durations.Duration
end

function ArrowTypes.toarrow(x::Avro.Duration)
    return Durations.Duration(x)
end

# The Julia type Arrow.jl stores a value of schema `s` as, inside a record. Union branches that Arrow stores
# alike, such as a string and an enum, share one type, since Arrow refuses a union of two equal children.
# `path` holds the records being typed, to refuse a recursive one.
function storagetype(s::Avro.Schema, ::Vector{Avro.RecordSchema})
    return Avro.juliatype(s)
end

function storagetype(::Avro.EnumSchema, ::Vector{Avro.RecordSchema})
    return String
end

function storagetype(s::Avro.BytesSchema, ::Vector{Avro.RecordSchema})
    return s.logical isa Avro.DecimalLogical ? decimaltype(s.logical) : Vector{UInt8}
end

function storagetype(s::Avro.FixedSchema, ::Vector{Avro.RecordSchema})
    s.logical isa Avro.DecimalLogical && return decimaltype(s.logical)
    T = Avro.juliatype(s)
    return T === Avro.Fixed ? Vector{UInt8} : T === Avro.Duration ? Durations.Duration : T
end

function storagetype(s::Avro.ArraySchema, path::Vector{Avro.RecordSchema})
    return Vector{storagetype(s.items, path)}
end

function storagetype(s::Avro.MapSchema, path::Vector{Avro.RecordSchema})
    return Dict{String,storagetype(s.values, path)}
end

function storagetype(s::Avro.UnionSchema, path::Vector{Avro.RecordSchema})
    return Union{(storagetype(b, path) for b in s.branches)...}
end

function storagetype(s::Avro.RecordSchema, path::Vector{Avro.RecordSchema})
    any(x -> x === s, path) && throw(ArgumentError("Arrow has no type for the recursive record $(Avro.fullname(s))"))
    push!(path, s)
    types = Tuple{(storagetype(f.schema, path) for f in s.fields)...}
    pop!(path)
    return NamedTuple{Tuple(s.fieldnames),types}
end

function decimaltype(l::Avro.DecimalLogical)
    l.precision <= 38 && return DataDecimals.Decimal{l.precision,l.scale,Int128}
    l.precision <= 76 && return DataDecimals.Decimal{l.precision,l.scale,DataDecimals.Int256}
    throw(ArgumentError("Arrow has no decimal type with precision $(l.precision); its widest holds 76 digits"))
end

# The value Arrow.jl stores for the generic value `x` of schema `s`.
function arrowvalue(::Avro.Schema, x)
    return x
end

function arrowvalue(::Avro.EnumSchema, x::Avro.EnumValue)
    return String(x)
end

function arrowvalue(::Avro.FixedSchema, x::Avro.Fixed)
    return x.bytes
end

function arrowvalue(::Avro.FixedSchema, x::Avro.Duration)
    return Durations.Duration(x)
end

function arrowvalue(s::Union{Avro.BytesSchema,Avro.FixedSchema}, x::Avro.Decimal)
    return decimaltype(s.logical)(x)
end

function arrowvalue(s::Union{Avro.BytesSchema,Avro.FixedSchema}, x::Avro.WideDecimal)
    return decimaltype(s.logical)(DataDecimals.DecimalValue{DataDecimals.Int256}(x.unscaled, x.scale))
end

function arrowvalue(::Avro.RecordSchema, x::Avro.Record)
    return ArrowTypes.toarrow(x)
end

function arrowvalue(s::Avro.ArraySchema, x::Vector)
    return map(v -> arrowvalue(s.items, v), x)
end

function arrowvalue(s::Avro.MapSchema, x::Dict{String})
    return Dict{String,Any}(k => arrowvalue(s.values, v) for (k, v) in x)
end

function arrowvalue(s::Avro.UnionSchema, x)
    x === missing && return missing
    i = findfirst(b -> holds(b, x), s.branches)
    i === nothing && throw(ArgumentError("no branch of the union holds the value $(repr(x))"))
    return arrowvalue(s.branches[i], x)
end

# Whether a decoded value can come from union branch `b`. Records, enums and fixed values match by schema name and
# decimals by scale, as Avro's encoder matches them; other values match by type.
function holds(b::Avro.Schema, x)
    x isa Union{Avro.Record,Avro.EnumValue,Avro.Fixed} && return b isa typeof(Avro.schema(x)) && b.name == Avro.schema(x).name
    x isa Avro.juliatype(b) || return false
    x isa Avro.Decimal && return DataDecimals.scale(x) == b.logical.scale
    x isa Avro.WideDecimal && return x.scale == b.logical.scale
    return true
end

end
