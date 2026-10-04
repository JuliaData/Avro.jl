# Column builders (plan §4.5): the column plan family decodes record fields straight into typed column
# vectors. Builders live in a schema-independent `Vector{ColumnBuilder}` and are driven by one loop with
# a function barrier per cell; every builder type is one member of the closed value set `E`
# (`valuetypes()`), so untrusted schemas never create new method instances.

abstract type ColumnBuilder end

struct Projection
    indices::Vector{Int}       # source field positions in caller order
    slots::Vector{Int}         # source field position -> projected slot (0 means skipped)
end

function projectedsyntheticvalue!(plan::RecordPlan, decoder::Decoder,
                                  projection::Projection,
                                  counter::SyntheticCounter)
    enter!(decoder)
    values = countwriter!(counter)
    try
        for (index, field) in enumerate(plan.fields)
            values = syntheticvalueadd(counter, values,
                                       syntheticvalue!(field, decoder,
                                                       projection.slots[index] != 0,
                                                       counter))
        end
    finally
        leave!(decoder)
    end
    return values
end

function projectionbytes(projection::Projection)
    return sizeof(Projection) + vectorbytes(Int, length(projection.indices)) +
           vectorbytes(Int, length(projection.slots))
end

function positionalprojection(selected::AbstractVector{Int}, nfields::Int, budget::Budget)
    charge = sizeof(Projection) + vectorbytes(Int, length(selected)) +
             vectorbytes(Int, nfields)
    reserve!(budget, charge)
    indices = Vector{Int}(undef, length(selected))
    allocated!(budget, vectorbytes(Int, length(selected)))
    slots = zeros(Int, nfields)
    allocated!(budget, vectorbytes(Int, nfields))
    for (slot, position) in enumerate(selected)
        1 <= position <= nfields || throw(ArgumentError("selected field position $position is out of range"))
        slots[position] == 0 || throw(ArgumentError("selected field position $position is duplicated"))
        indices[slot] = position
        slots[position] = slot
    end
    projection = Projection(indices, slots)
    allocated!(budget, sizeof(Projection))
    return projection
end

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
    fastkinds::Vector{UInt8} # closed leaf instruction set; zero delegates to the concrete plan
    fastbytes::Int          # the run's total fixed width, or -1 when any member is dynamic
    fastmaxbytes::Int       # largest max_bytes-governed fixed value in a fixed-width run
end

const SKIP_FALLBACK = UInt8(0)
const SKIP_LENGTH = UInt8(1)
const SKIP_DOUBLE = UInt8(2)
const SKIP_LONG = UInt8(3)
const SKIP_INT = UInt8(4)
const SKIP_BOOL = UInt8(5)
const SKIP_FLOAT = UInt8(6)
const SKIP_NULL = UInt8(7)

"The parameter-free fast skip instruction for one common leaf plan."
function fastskipkind(@nospecialize(p::ReadPlan))
    return p isa Union{StringPlan,BytesPlan} ? SKIP_LENGTH :
        p isa DoublePlan ? SKIP_DOUBLE :
        p isa Union{LongPlan,TimestampPlan,LocalTimestampPlan} ? SKIP_LONG :
        p isa Union{IntPlan,DatePlan} ? SKIP_INT :
        p isa BoolPlan ? SKIP_BOOL :
        p isa FloatPlan ? SKIP_FLOAT :
        p isa NullPlan ? SKIP_NULL : SKIP_FALLBACK
end

"The package-owned heap storage of one builder held behind `ColumnBuilder`."
function columnnodebytes(::Type{C}) where {C<:ColumnBuilder}
    return Base.issingletontype(C) ? 0 : boxbytes(C)
end

"The builder-vector and concrete-node storage, excluding typed column data and payload."
function columnbuildersstate(cols::Vector{ColumnBuilder})
    charge = vectorbytes(ColumnBuilder, length(cols))
    for col in cols
        charge = checked_add(charge, columnnodebytes(typeof(col)))
    end
    return charge
end

"The retained builder state for one unresolved record plan, excluding column data."
function columnbuildersstate(plan::RecordPlan,
                             projection::Union{Nothing,Projection}=nothing)
    charge = vectorbytes(ColumnBuilder, length(plan.fields))
    for (index, fieldplan) in enumerate(plan.fields)
        if projection === nothing || projection.slots[index] != 0
            E = juliatype(plan.schema.fields[index].schema)
            charge = checked_add(
                charge, columnnodebytes(TypedColumn{E,typeof(fieldplan)}))
        else
            charge = checked_add(
                charge, columnnodebytes(SkipColumn{typeof(fieldplan)}))
        end
    end
    return charge
end

"The exact fused skip-view state that can coexist with one unresolved builder set."
function fusedstate(plan::RecordPlan, projection::Union{Nothing,Projection})
    projection === nothing && return 0
    outlen = 0
    charge = 0
    hasrun = false
    index = 1
    while index <= length(plan.fields)
        if projection.slots[index] == 0
            first = index
            while index <= length(plan.fields) && projection.slots[index] == 0
                index += 1
            end
            runlength = index - first
            if runlength > 1
                hasrun = true
                charge = checked_add(charge, columnnodebytes(SkipRun))
                charge = checked_add(charge, vectorbytes(ReadPlan, runlength))
                charge = checked_add(charge, vectorbytes(UInt8, runlength))
            end
        else
            index += 1
        end
        outlen += 1
    end
    hasrun || return 0
    return checked_add(vectorbytes(ColumnBuilder, outlen), charge)
end

"The builder construction/decode peak outside the typed column vectors and decoded payload."
function columnbuilderpeakstate(plan::RecordPlan,
                                projection::Union{Nothing,Projection})
    return checked_add(columnbuildersstate(plan, projection),
                       fusedstate(plan, projection))
end

function releasecolumnbuilders!(cols::Vector{ColumnBuilder}, budget::Budget)
    release!(budget, columnbuildersstate(cols))
    return nothing
end

function setskipcolumn!(cols::Vector{ColumnBuilder}, index::Int, plan::P,
                        budget::Budget) where {P<:ReadPlan}
    charge = columnnodebytes(SkipColumn{P})
    reserve!(budget, charge)
    cols[index] = SkipColumn{P}(plan)
    allocated!(budget, charge)
    return nothing
end

"The fixed encoded width of a skipped leaf, or -1 (varints, length-prefixed and nested values)."
function skipwidth(@nospecialize(p::ReadPlan))
    return p isa NullPlan ? 0 :
        p isa FloatPlan ? 4 :
        p isa DoublePlan ? 8 :
        p isa DurationPlan ? 12 :
        p isa UUIDFixedPlan ? 16 :
        p isa FixedPlan ? p.schema.size : -1
end

"The encoded width governed by max_bytes for one fixed leaf, or zero for plain primitives."
function skipmaxbytes(@nospecialize(p::ReadPlan))
    return p isa FixedPlan ? p.schema.size :
        p isa DurationPlan ? 12 : p isa UUIDFixedPlan ? 16 : 0
end

"The new vector, run nodes and plan vectors owned by a fused view of `cols`."
function fusedstate(cells::Vector{ColumnBuilder}, cols::Vector{ColumnBuilder})
    cells === cols && return 0
    charge = vectorbytes(ColumnBuilder, length(cells))
    for cell in cells
        cell isa SkipRun || continue
        charge = checked_add(charge, columnnodebytes(SkipRun))
        charge = checked_add(charge, vectorbytes(ReadPlan, length(cell.plans)))
        charge = checked_add(charge, vectorbytes(UInt8, length(cell.fastkinds)))
    end
    return charge
end

function releasefused!(cells::Vector{ColumnBuilder}, cols::Vector{ColumnBuilder}, budget::Budget)
    release!(budget, fusedstate(cells, cols))
    return nothing
end

"Fuse consecutive `SkipColumn`s of `cols` into an exactly allocated, budgeted skip view."
function fuseskips(cols::Vector{ColumnBuilder}, budget::Union{Nothing,Budget}=nothing)
    outlen = 0
    hasrun = false
    i = 1
    while i <= length(cols)
        if cols[i] isa SkipColumn && i < length(cols) && cols[i + 1] isa SkipColumn
            while i <= length(cols) && cols[i] isa SkipColumn
                i += 1
            end
            hasrun = true
        else
            i += 1
        end
        outlen += 1
    end
    hasrun || return cols
    charge = vectorbytes(ColumnBuilder, outlen)
    i = 1
    while i <= length(cols)
        if cols[i] isa SkipColumn && i < length(cols) && cols[i + 1] isa SkipColumn
            first = i
            while i <= length(cols) && cols[i] isa SkipColumn
                i += 1
            end
            charge = checked_add(charge, vectorbytes(ReadPlan, i - first))
            charge = checked_add(charge, vectorbytes(UInt8, i - first))
            charge = checked_add(charge, columnnodebytes(SkipRun))
        else
            i += 1
        end
    end
    checkpoint = budget === nothing ? nothing : budgetcheckpoint(budget)
    out = nothing
    try
        budget === nothing || reserve!(budget, charge)
        out = Vector{ColumnBuilder}(undef, outlen)
        budget === nothing || allocated!(budget, vectorbytes(ColumnBuilder, outlen))
        i = 1
        dest = 0
        while i <= length(cols)
            dest += 1
            if cols[i] isa SkipColumn && i < length(cols) && cols[i + 1] isa SkipColumn
                first = i
                while i <= length(cols) && cols[i] isa SkipColumn
                    i += 1
                end
                plans = Vector{ReadPlan}(undef, i - first)
                fastkinds = Vector{UInt8}(undef, i - first)
                budget === nothing || allocated!(budget, vectorbytes(ReadPlan, length(plans)))
                budget === nothing || allocated!(budget, vectorbytes(UInt8, length(fastkinds)))
                width = 0
                maxbytes = 0
                for (offset, source) in enumerate(first:i - 1)
                    plan = (cols[source]::SkipColumn).plan
                    plans[offset] = plan
                    fastkinds[offset] = fastskipkind(plan)
                    w = skipwidth(plan)
                    width = (width < 0 || w < 0) ? -1 : satadd(width, w)
                    maxbytes = max(maxbytes, skipmaxbytes(plan))
                end
                out[dest] = SkipRun(plans, fastkinds, width, maxbytes)
                budget === nothing || allocated!(budget, columnnodebytes(SkipRun))
            else
                out[dest] = cols[i]
                i += 1
            end
        end
        return out
    catch
        out = nothing
        budget === nothing || rollbackreservations!(budget, checkpoint::NTuple{2,Int})
        rethrow()
    end
end

"""
    columnbuilders(plan::RecordPlan, selected, capacity, budget) -> Vector{ColumnBuilder}

One builder per schema field in schema order: a `TypedColumn` for every field whose position is in
`selected` (`nothing` selects all), a `SkipColumn` otherwise. Column storage for `capacity` rows is
charged exactly at allocation (`Base.elsize` per slot plus one tag byte per isbits-`Union` element).
"""
function columnbuilders(p::RecordPlan, projection::Union{Nothing,Projection}, capacity::Int,
                        budget::Budget)
    capacity >= 0 || throw(ArgumentError("capacity must be non-negative"))
    checkpoint = budgetcheckpoint(budget)
    cols = nothing
    try
        outer = vectorbytes(ColumnBuilder, length(p.fields))
        reserve!(budget, outer)
        cols = Vector{ColumnBuilder}(undef, length(p.fields))
        allocated!(budget, outer)
        for (i, f) in enumerate(p.schema.fields)
            if projection === nothing || projection.slots[i] != 0
                cols[i] = makecolumn(juliatype(f.schema), p.fields[i], capacity, budget)
            else
                setskipcolumn!(cols, i, p.fields[i], budget)
            end
        end
        return cols
    catch
        cols = nothing
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

function columnbuilders(p::RecordPlan, selected::AbstractVector{Int}, capacity::Int,
                        budget::Budget)
    projection = positionalprojection(selected, length(p.fields), budget)
    try
        return columnbuilders(p, projection, capacity, budget)
    finally
        release!(budget, projectionbytes(projection))
    end
end

function makecolumn(::Type{E}, plan::P, capacity::Int, budget::Budget) where {E,P<:ReadPlan}
    checkpoint = budgetcheckpoint(budget)
    data = nothing
    try
        databytes = vectorbytes(E, capacity)
        node = columnnodebytes(TypedColumn{E,P})
        reserve!(budget, checked_add(databytes, node))
        data = Vector{E}(undef, capacity)
        allocated!(budget, databytes)
        column = TypedColumn{E,P}(plan, data, 0)
        allocated!(budget, node)
        return column
    catch
        data = nothing
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
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
            checkvaluebytes(d.budget, c.fastmaxbytes)
            bytesavailable(d, c.fastbytes) ||
                dataerror(d, "fixed run of $(c.fastbytes) bytes exceeds the remaining $(remaining(d)) bytes")
            d.pos += c.fastbytes
            return nothing
        end
        b = d.budget
        for i in eachindex(c.fastkinds)                 # parameter-free closed leaf instruction set
            kind = @inbounds c.fastkinds[i]
            if kind == SKIP_LENGTH
                countvalues!(b)
                skiplen(d)
            elseif kind == SKIP_DOUBLE
                countvalues!(b)
                readdouble(d)
            elseif kind == SKIP_LONG
                countvalues!(b)
                readlong(d)
            elseif kind == SKIP_INT
                countvalues!(b)
                readint(d)
            elseif kind == SKIP_BOOL
                countvalues!(b)
                readbool(d)                            # bools stay domain-checked in both modes (§4.3)
            elseif kind == SKIP_FLOAT
                countvalues!(b)
                readfloat(d)
            elseif kind == SKIP_NULL
                countvalues!(b)
            else
                skip(@inbounds(c.plans[i]), d)
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
function decodecolumns(p::RecordPlan, d::Decoder, nrows::Int; selected=nothing)
    cols = columnbuilders(p, selected, nrows, d.budget)
    for _ in 1:nrows
        countvalues!(d.budget)
        decoderow!(cols, d)
    end
    return cols
end
