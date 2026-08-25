# Column builders (plan §4.5): the column plan family decodes record fields straight into typed column
# vectors. Builders live in a schema-independent `Vector{ColumnBuilder}` and are driven by one loop with
# a function barrier per cell; every builder type is one member of the closed value set `E`
# (`valuetypes()`), so untrusted schemas never create new method instances.

abstract type ColumnBuilder end

"Check the selected output produced through `rows`: completed slots plus retained payload, not chunk capacity."
function checkblockoutput(budget::Budget, baseline::Int, rows::Int, slotrow::Int, cap::Int)
    output = checked_add(max(budget.reserved - baseline, 0), checked_mul(rows, slotrow))
    output <= cap || throw(limiterror(budget, :max_block_output_bytes, output, cap))
    return output
end

"A selected field decoded into a `Vector{E}` (`E = juliatype(field schema)`) by its plan `P`."
mutable struct TypedColumn{E,P<:ReadPlan} <: ColumnBuilder
    const plan::P
    data::Vector{E}
    len::Int
end

"A field that is not selected: skipped under the active validation mode."
struct SkipColumn{P<:ReadPlan} <: ColumnBuilder
    plan::P                 # concrete: the skip devirtualizes (§10.2 projection work)
end

"""
Consecutive unselected fields fused into one cell: in `:fast` mode a run of fixed-width leaves is
jumped in a single bounds check with its values counted in bulk (identical work-rule arithmetic);
otherwise — and always in `:strict` mode — every member is skipped and validated individually.
"""
struct SkipRun <: ColumnBuilder
    plans::Vector{ReadPlan}
    fastbytes::Int          # the run's total fixed width, or -1 when any member is dynamic
end

"The fixed encoded width of a skipped leaf, or -1 (varints, length-prefixed and nested values)."
function skipwidth(@nospecialize(p::ReadPlan))
    return p isa NullPlan ? 0 :
        p isa BoolPlan ? 1 :
        p isa FloatPlan ? 4 :
        p isa DoublePlan ? 8 :
        p isa DurationPlan ? 12 :
        p isa UUIDFixedPlan ? 16 :
        p isa FixedPlan ? p.schema.size : -1
end

"Fuse consecutive `SkipColumn`s of `cols` into `SkipRun`s (the per-field vector stays untouched)."
function fuseskips(cols::Vector{ColumnBuilder})
    out = ColumnBuilder[]
    i = 1
    while i <= length(cols)
        if cols[i] isa SkipColumn && i < length(cols) && cols[i + 1] isa SkipColumn
            plans = ReadPlan[]
            width = 0
            while i <= length(cols) && cols[i] isa SkipColumn
                p = (cols[i]::SkipColumn).plan
                push!(plans, p)
                w = skipwidth(p)
                width = (width < 0 || w < 0) ? -1 : width + w
                i += 1
            end
            push!(out, SkipRun(plans, width))
        else
            push!(out, cols[i])
            i += 1
        end
    end
    return out
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
    c = TypedColumn{E,P}(plan, Vector{E}(undef, capacity), 0)
    allocated!(budget, vectorbytes(E, capacity))
    return c
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

function decodecell!(c::SkipColumn, d::Decoder)
    return skip(c.plan, d)
end

function decodecell!(c::SkipRun, d::Decoder)
    if d.validate === :fast
        if c.fastbytes >= 0
            countvalues!(d.budget, length(c.plans))
            skipfixed(d, c.fastbytes)
            return nothing
        end
        b = d.budget
        for p in c.plans                               # devirtualized common leaves (closed kind set)
            if p isa StringPlan || p isa BytesPlan
                countvalues!(b)
                skiplen(d)
            elseif p isa DoublePlan
                countvalues!(b)
                skipfixed(d, 8)
            elseif p isa Union{LongPlan,TimestampPlan,LocalTimestampPlan}
                countvalues!(b)
                readlong(d)
            elseif p isa Union{IntPlan,DatePlan}
                countvalues!(b)
                readint(d)
            elseif p isa BoolPlan
                countvalues!(b)
                readbool(d)                            # bools stay domain-checked in both modes (§4.3)
            elseif p isa FloatPlan
                countvalues!(b)
                skipfixed(d, 4)
            elseif p isa NullPlan
                countvalues!(b)
            else
                skip(p, d)
            end
        end
        return nothing
    end
    for p in c.plans
        skip(p, d)
    end
    return nothing
end

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
    allocated!(budget, vectorbytes(E, newcap))
    copyto!(nd, 1, c.data, 1, c.len)
    oldbytes = vectorbytes(E, length(c.data))
    c.data = nd                                        # the old storage is unreachable only after the rebind
    release!(budget, oldbytes)
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
        allocated!(budget, vectorbytes(E, c.len))
        copyto!(out, 1, c.data, 1, c.len)
        oldbytes = vectorbytes(E, length(c.data))
        c.data = out                                   # the old storage is unreachable only after the rebind
        release!(budget, oldbytes)
    end
    return out
end

function rowcount(c::TypedColumn)
    return c.len
end

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
