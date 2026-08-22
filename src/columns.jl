# Column builders (plan §4.5): the column plan family decodes record fields straight into typed column
# vectors. Builders live in a schema-independent `Vector{ColumnBuilder}` and are driven by one loop with
# a function barrier per cell; every builder type is one member of the closed value set `E`
# (`valuetypes()`), so untrusted schemas never create new method instances.

abstract type ColumnBuilder end

"A selected field decoded into a `Vector{E}` (`E = juliatype(field schema)`) by its plan `P`."
mutable struct TypedColumn{E,P<:ReadPlan} <: ColumnBuilder
    const plan::P
    data::Vector{E}
    len::Int
end

"A field that is not selected: skipped under the active validation mode."
struct SkipColumn <: ColumnBuilder
    plan::ReadPlan
end

"""
    columnbuilders(plan::RecordPlan, selected, capacity, budget) -> Vector{ColumnBuilder}

One builder per schema field in schema order: a `TypedColumn` for every field whose position is in
`selected` (`nothing` selects all), a `SkipColumn` otherwise. Column storage for `capacity` rows is
charged exactly at allocation (`Base.elsize` per slot plus one tag byte per isbits-`Union` element).
"""
function columnbuilders(p::RecordPlan, selected::Union{Nothing,AbstractVector{Int}}, capacity::Int, budget::Budget)
    capacity >= 0 || throw(ArgumentError("capacity must be non-negative"))
    cols = Vector{ColumnBuilder}(undef, length(p.fields))
    for (i, f) in enumerate(p.schema.fields)
        if selected === nothing || i in selected
            cols[i] = makecolumn(juliatype(f.schema), p.fields[i], capacity, budget)
        else
            cols[i] = SkipColumn(p.fields[i])
        end
    end
    return cols
end

function makecolumn(::Type{E}, plan::P, capacity::Int, budget::Budget) where {E,P<:ReadPlan}
    reserve!(budget, vectorbytes(E, capacity))
    return TypedColumn{E,P}(plan, Vector{E}(undef, capacity), 0)
end

"""
    decoderow!(cols::Vector{ColumnBuilder}, d::Decoder)

Decode one record (the schema's field order) into the builders: one dynamic dispatch per cell.
"""
function decoderow!(cols::Vector{ColumnBuilder}, d::Decoder)
    enter!(d)
    for c in cols
        decodecell!(c, d)
    end
    leave!(d)
    return nothing
end
decodecell!(c::SkipColumn, d::Decoder) = skip(c.plan, d)

function decodecell!(c::TypedColumn{E}, d::Decoder) where {E}
    return appendcell!(c, d.budget, decode(c.plan, d))
end

function appendcell!(c::TypedColumn{E}, budget::Budget, v) where {E}
    n = c.len + 1
    if n > length(c.data)
        grow!(c, budget, max(2 * length(c.data), 4))
    end
    @inbounds c.data[n] = v
    c.len = n
    return nothing
end

function grow!(c::TypedColumn{E}, budget::Budget, newcap::Int) where {E}
    reserve!(budget, vectorbytes(E, newcap))
    nd = Vector{E}(undef, newcap)
    copyto!(nd, 1, c.data, 1, c.len)
    release!(budget, vectorbytes(E, length(c.data)))
    c.data = nd
    return nothing
end

"""
    finishcolumn!(c::TypedColumn, budget) -> Vector

The column trimmed to its row count (over-capacity storage released from the budget).
"""
function finishcolumn!(c::TypedColumn{E}, budget::Budget) where {E}
    out = c.data
    if c.len != length(out)
        reserve!(budget, vectorbytes(E, c.len))
        out = Vector{E}(undef, c.len)
        copyto!(out, 1, c.data, 1, c.len)
        release!(budget, vectorbytes(E, length(c.data)))
        c.data = out
    end
    return out
end

rowcount(c::TypedColumn) = c.len

"""
    decodecolumns(plan::RecordPlan, d::Decoder, nrows; selected=nothing) -> Vector{ColumnBuilder}

Decode `nrows` consecutive records from `d` into column builders.
"""
function decodecolumns(p::RecordPlan, d::Decoder, nrows::Int; selected::Union{Nothing,AbstractVector{Int}}=nothing)
    cols = columnbuilders(p, selected, nrows, d.budget)
    for _ in 1:nrows
        countvalues!(d.budget)
        decoderow!(cols, d)
    end
    return cols
end
