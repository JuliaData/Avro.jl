mutable struct RowType{T} <: NamedType
    type::String
    namespace::String
    name::String
    doc::Union{String, Nothing}
    aliases::Union{Vector{String}, Nothing}
    fields::Vector{FieldType}
    schema::T # Tables.Schema
end

==(x::RowType, y::RowType) = x.name == y.name && x.fields == y.fields
RowType(name, fields::Vector{FieldType}, sch::Tables.Schema, namespace="", doc=nothing, aliases=nothing) =
    RowType("record", namespace, name, doc, aliases, fields, sch)
RowType() = RowType("record", "", "", nothing, nothing, FieldType[])
StructTypes.StructType(::Type{<:RowType}) = StructTypes.Mutable()
StructTypes.excludes(::Type{<:RowType}) = (:schema,)

Tables.Schema(::Type{Record{names, types, N}}) where {names, types, N} = Tables.Schema{names, types}()

schematype(::Type{T}) where {T <: Tables.Schema} = schematype(T())
schematype(sch::Tables.Schema{names, types}, name=autoname(Record{names, types, fieldcount(types)})) where {names, types} =
    RowType(name, FieldType[FieldType(String(names[i]), schematype(fieldtype(types, i))) for i = 1:length(names)], sch)

mutable struct RowWriteClosure{B, S, T, KW}
    type::RowType{S}
    buf::T
    pos::Int
    len::Int
    opts::KW
end

function (f::RowWriteClosure{B, S, T, KW})(val, i, nm) where {B, S, T, KW}
    # @debug 2 "writing $typ at pos = $(f.pos)"
    f.pos = writevalue(B(), f.type.fields[i].type, val, f.buf, f.pos, f.len, f.opts)
end

function writevalue(B::Binary, T::RowType{S}, row, buf, pos, len, opts) where {S}
    c = RowWriteClosure{Binary, S, typeof(buf), typeof(opts)}(T, buf, pos, len, opts)
    Tables.eachcolumn(c, T.schema, row)
    return c.pos
end

mutable struct NBytesRowClosure{S}
    RT::RowType{S}
    n::Int
end

function (f::NBytesRowClosure{S})(val, i, nm) where {S}
    f.n += nbytes(f.RT.fields[i].type, val)
end

function nbytes(RT::RowType, row)
    c = NBytesRowClosure(RT, 0)
    Tables.eachcolumn(c, RT.schema, row)
    return c.n
end
