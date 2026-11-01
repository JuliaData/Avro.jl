# Arrow.jl stores a nested record as a struct of its fields. An `Avro.Record` has no field types of its own,
# so its schema gives the struct type, which also lets Arrow build the placeholder values it stores under a
# missing struct. A nested UUID is stored as its text because Arrow.jl has no such placeholder for `UUID`.
module AvroArrowTypesExt

using Avro: Avro
using UUIDs: UUID
import ArrowTypes

ArrowTypes.ArrowType(::Type{Avro.Record}) = NamedTuple
ArrowTypes.default(::Type{Avro.Duration}) = Avro.Duration(UInt32(0), UInt32(0), UInt32(0))

# Arrow converts a value each time it reads one, which for a struct is once per field: a record column
# converts once instead.
function ArrowTypes.ToArrow(x::Union{AbstractVector{Avro.Record},AbstractVector{Union{Missing,Avro.Record}}})
    return ArrowTypes.ToArrow(map(r -> r === missing ? missing : ArrowTypes.toarrow(r), x))
end

function ArrowTypes.toarrow(r::Avro.Record)
    s = Avro.schema(r)
    T = arrowtype(s, Avro.RecordSchema[])
    return T(Tuple(arrowvalue(getfield(r, :values)[i]) for i in eachindex(s.fields)))
end

arrowtype(s::Avro.Schema, ::Vector{Avro.RecordSchema}) = Avro.juliatype(s)
arrowtype(::Avro.EnumSchema, ::Vector{Avro.RecordSchema}) = String
arrowtype(s::Avro.ArraySchema, path::Vector{Avro.RecordSchema}) = Vector{arrowtype(s.items, path)}
arrowtype(s::Avro.MapSchema, path::Vector{Avro.RecordSchema}) = Dict{String,arrowtype(s.values, path)}

function arrowtype(s::Avro.StringSchema, ::Vector{Avro.RecordSchema})
    return String
end

function arrowtype(s::Avro.FixedSchema, ::Vector{Avro.RecordSchema})
    T = Avro.juliatype(s)
    return T === Avro.Fixed ? Vector{UInt8} : T === UUID ? String : T
end

function arrowtype(s::Avro.UnionSchema, path::Vector{Avro.RecordSchema})
    nullable = Avro.nullablebranch(s)
    return nullable == 0 ? Any : Union{Missing,arrowtype(s.branches[3 - nullable], path)}
end

function arrowtype(s::Avro.RecordSchema, path::Vector{Avro.RecordSchema})
    any(x -> x === s, path) && throw(ArgumentError("Arrow has no type for the recursive record $(Avro.fullname(s))"))
    push!(path, s)
    types = Tuple{(arrowtype(f.schema, path) for f in s.fields)...}
    pop!(path)
    return NamedTuple{Tuple(s.fieldnames),types}
end

arrowvalue(x) = x
arrowvalue(x::Avro.EnumValue) = String(x)
arrowvalue(x::UUID) = string(x)
arrowvalue(x::Avro.Fixed) = x.bytes
arrowvalue(x::Avro.Record) = ArrowTypes.toarrow(x)
arrowvalue(x::Vector{UInt8}) = x
arrowvalue(x::Vector) = map(arrowvalue, x)
arrowvalue(x::Dict{String}) = Dict{String,Any}(k => arrowvalue(v) for (k, v) in x)

end
