# 1.x deprecation shims (plan §7): kept for one major cycle, removal in 3.0. Tested against real
# files written by the pinned Avro.jl 1.1.2 (test/fixtures/generated/legacy1x).

"""
    Avro.readtable(source; kw...)

Deprecated 1.x entry point: reads an object container file as a column table with `legacy=:avrojl1`
(tolerating the 1.x writer's defects). Use [`Avro.Table`](@ref) — with `legacy=:avrojl1` only for
files written by Avro.jl ≤ 1.1.2, and `decimal_byteorder=:little` if they contain decimals.
"""
function readtable(source; kw...)
    Base.depwarn("`Avro.readtable(src)` is deprecated; use `Avro.Table(src)` " *
                 "(with `legacy=:avrojl1` for files written by Avro.jl ≤ 1.1.2).", :readtable)
    return Table(source; legacy=:avrojl1, kw...)
end

"""
    Avro.writetable(dst, table; compress=nothing, kw...)

Deprecated 1.x entry point. Use [`Avro.write`](@ref) — `compress=:zstd` maps to `codec=:zstandard`.
"""
function writetable(dst, table; compress::Union{Nothing,Symbol}=nothing, kw...)
    Base.depwarn("`Avro.writetable(dst, tbl; compress=...)` is deprecated; use " *
                 "`Avro.write(dst, tbl; codec=...)` (`:zstd` is now `:zstandard`).", :writetable)
    codecname = compress === nothing ? :null : compress === :zstd ? :zstandard : compress
    return write(dst, table; codec=codecname, kw...)
end

"""
    Avro.write(x; schema=nothing, limits=Limits())

Deprecated 1.x datum writer. Use [`Avro.encode`](@ref), or [`Avro.encode!`](@ref) for an `IO`.
"""
function write(x; schema=nothing, limits::Limits=Limits())
    Base.depwarn("`Avro.write(x; schema=...)` is deprecated; use `Avro.encode(schema, x)`.", :write)
    schema === nothing && return encode(x; limits=limits)
    s = deprecatedschema(schema, limits)
    return encode(s, x; limits=limits)
end

"""
    Avro.read(source, T_or_schema)

Deprecated 1.x datum reader. Use [`Avro.decode`](@ref) or a prepared [`Avro.DatumReader`](@ref).
"""
function read(src, s::Schema; limits::Limits=Limits(), kw...)
    Base.depwarn("`Avro.read(src, schema)` is deprecated; use `Avro.decode(schema, src)`.", :read)
    return deprecateddecode(src, s; limits=limits, kw...)
end

function read(src, ::Type{T}; schema=nothing, limits::Limits=Limits(), kw...) where {T}
    Base.depwarn("`Avro.read(src, T)` is deprecated; use `Avro.decode(schema, src, T)`.", :read)
    s = schema === nothing ? Avro.schema(T; limits=limits) : deprecatedschema(schema, limits)
    return deprecateddecode(src, s, T; limits=limits, kw...)
end

function deprecatedschema(s, limits::Limits)
    s isa Schema && return s
    s isa Type && return Avro.schema(s; limits=limits)
    throw(ArgumentError("`schema` must be an `Avro.Schema` or Julia type"))
end

function deprecateddecode(src::AbstractString, s::Schema, args...; kw...)
    return open(src, "r") do io
        decode(s, io, args...; kw...)
    end
end

function deprecateddecode(src, s::Schema, args...; kw...)
    return decode(s, src, args...; kw...)
end
