@enum SortOrder ascending descending ignore

mutable struct FieldType
    name::String
    doc::Union{String, Nothing}
    type::Schema
    default::Any
    order::SortOrder
    aliases::Union{Vector{String}, Nothing}
end

==(x::FieldType, y::FieldType) = x.name == y.name && x.type == y.type
FieldType(name::String, type::Schema, doc=nothing, default=nothing, order=ascending, aliases=nothing) =
    FieldType(name, doc, type, default, order, aliases)
FieldType() = FieldType("", nothing, "", nothing, ascending, nothing)
StructTypes.StructType(::Type{FieldType}) = StructTypes.Mutable()
StructTypes.omitempties(::Type{FieldType}) = true

mutable struct RecordType <: NamedType
    type::String
    namespace::String
    name::String
    doc::Union{String, Nothing}
    aliases::Union{Vector{String}, Nothing}
    fields::Vector{FieldType}
end

==(x::RecordType, y::RecordType) = x.name == y.name && x.fields == y.fields
RecordType(name, fields::Vector{FieldType}, namespace="", doc=nothing, aliases=nothing) =
    RecordType("record", namespace, name, doc, aliases, fields)
RecordType() = RecordType("record", "", "", nothing, nothing, FieldType[])
StructTypes.StructType(::Type{RecordType}) = StructTypes.Mutable()

struct Record{names, T, N} <: Tables.AbstractRow
    type::RecordType
    data::Vector{UInt8}
    values::NTuple{N, Int} # starting byte positions for each field in `data`
end

type(x::Record) = getfield(x, :type)
data(x::Record) = getfield(x, :data)
values(x::Record) = getfield(x, :values)
Base.NamedTuple(x::Record{names, types, N}) where {names, types, N} =
    NamedTuple{names, types}(
        ntuple(i -> readvalue(Binary(), type(x).fields[i].type, fieldtype(types, i), data(x), values(x)[i], length(data(x)), (;))[1], Val(N))
    )

juliatype(x::RecordType) = Record{symtup(map(f->f.name, x.fields)), Tuple{map(f->juliatype(f.type), x.fields)...}, length(x.fields)}

autoname(T::Type{Record{names, types, N}}) where {names, types, N} =
    Base.string("Record_", hash(T))

schematype(::StructTypes.CustomStruct, ::Type{Record{names, types, N}}, name=autoname(Record{names, types, N})) where {names, types, N} =
    schematype(Record{names, types})
schematype(::StructTypes.CustomStruct, ::Type{Record{names, types}}, name=autoname(Record{names, types, fieldcount(types)})) where {names, types} =
    RecordType(name, FieldType[FieldType(String(names[i]), schematype(fieldtype(types, i))) for i = 1:length(names)])
schematype(::Type{NamedTuple{names, types}}) where {names, types} =
    schematype(StructTypes.CustomStruct(), Record{names, types})
schematype(::StructTypes.DataType, ::Type{T}) where {T} =
    schematype(StructTypes.CustomStruct(), Record{fieldnames(T), Tuple{fieldtypes(T)...}}, Base.string(T))
schematype(::StructTypes.CustomStruct, ::Type{T}) where {T} =
    schematype(StructTypes.lowertype(T))
schematype(::StructTypes.ArrayType, ::Type{T}) where {T <: Tuple} =
    schematype(NamedTuple{symtup(1:fieldcount(T)), T})

StructTypes.StructType(::Type{<:Record}) = StructTypes.CustomStruct()
StructTypes.lower(x::Record) = NamedTuple(x)

Tables.columnnames(::Record{names}) where {names} = names
Tables.getcolumn(x::Record{names, T}, ::Type{S}, i::Int, nm::Symbol) where {names, T, S} =
    readvalue(Binary(), S, data(x), values(x)[i], length(data(x)), (;))[1]
Tables.getcolumn(x::Record{names, T}, i::Int) where {names, T} =
    readvalue(Binary(), fieldtype(T, i), data(x), values(x)[i], length(data(x)), (;))[1]
function Tables.getcolumn(x::Record{names, T}, nm::Symbol) where {names, T}
    i = Tables.columnindex(names, nm)
    return readvalue(Binary(), fieldtype(T, i), data(x), values(x)[i], length(data(x)), (;))[1]
end

mutable struct WriteClosure{B, T, KW}
    type::RecordType
    buf::T
    pos::Int
    len::Int
    opts::KW
end

function (f::WriteClosure{B, T, KW})(i, nm, typ, v) where {B, T, KW}
    @debug 2 "writing $typ at pos = $(f.pos)"
    f.pos = writevalue(B(), f.type.fields[i].type, v, f.buf, f.pos, f.len, f.opts)
end

function writevalue(B::Binary, T::RecordType, x, buf, pos, len, opts)
    c = WriteClosure{Binary, typeof(buf), typeof(opts)}(T, buf, pos, len, opts)
    StructTypes.foreachfield(c, x)
    return c.pos
end

mutable struct NBytesClosure
    RT::RecordType
    n::Int
end

function (f::NBytesClosure)(i, nm, typ, v)
    f.n += nbytes(f.RT.fields[i].type, v)
end

function nbytes(RT::RecordType, x)
    c = NBytesClosure(RT, 0)
    StructTypes.foreachfield(c, x)
    return c.n
end

mutable struct ReadClosure{B, T, KW}
    type::RecordType
    buf::T
    pos::Int
    len::Int
    opts::KW
end

function (f::ReadClosure{B, T, KW})(i, nm, ::Type{S}) where {B, T, KW, S}
    x_i, pos = readvalue(B(), f.type.fields[i].type, S, f.buf, f.pos, f.len, f.opts)
    f.pos = pos
    return x_i
end

function readvalue(B::Binary, RT::RecordType, ::Type{T}, buf, pos, len, opts) where {T}
    c = ReadClosure{Binary, typeof(buf), typeof(opts)}(RT, buf, pos, len, opts)
    x = StructTypes.construct(c, T)
    return x, c.pos
end

mutable struct ReadSkipClosure{B, T, KW}
    type::RecordType
    buf::T
    pos::Int
    len::Int
    opts::KW
end

function (f::ReadSkipClosure{B, T, KW})(i, nm, ::Type{S}) where {B, T, KW, S}
    f.pos = skipvalue(B(), f.type.fields[i].type, S, f.buf, f.pos, f.len, f.opts)
    return
end

function skipvalue(B::Binary, RT::RecordType, ::Type{T}, buf, pos, len, opts) where {T}
    c = ReadSkipClosure{Binary, typeof(buf), typeof(opts)}(RT, buf, pos, len, opts)
    x = StructTypes.foreachfield(c, T)
    return c.pos
end

mutable struct PosClosure
    pos::Int
end

function (f::PosClosure)(RT, buf, len, opts, i, ::Type{S}) where {S}
    pos = f.pos
    f.pos = skipvalue(Binary(), RT.fields[i].type, S, buf, pos, len, opts)
    return pos
end

readvalue(B::Binary, RT::RecordType, ::Type{Record{names, types}}, buf, pos, len, opts) where {names, types} =
    readvalue(B, RT, Record{names, types, fieldcount(types)}, buf, pos, len, opts)

function readvalue(B::Binary, RT::RecordType, ::Type{Record{names, types, N}}, buf, pos, len, opts) where {names, types, N}
    c = PosClosure(pos)
    values = ntuple(i -> c(RT, buf, len, opts, i, fieldtype(types, i)), Val(N))
    return Record{names, types, N}(RT, buf, values), c.pos
end

skipvalue(B::Binary, RT::RecordType, ::Type{Record{names, types}}, buf, pos, len, opts) where {names, types} =
    skipvalue(B, RT, Record{names, types, fieldcount(types)}, buf, pos, len, opts)

function skipvalue(B::Binary, RT::RecordType, ::Type{Record{names, types, N}}, buf, pos, len, opts) where {names, types, N}
    c = PosClosure(pos)
    values = ntuple(i -> c(RT, buf, len, opts, i, fieldtype(types, i)), Val(fieldcount(types)))
    return c.pos
end
