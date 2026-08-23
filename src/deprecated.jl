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
