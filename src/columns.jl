# Column builders: a record's fields decoded straight into typed column vectors. Builders live in a
# schema-independent `Vector{ColumnBuilder}` driven by one loop with a function barrier per cell; every
# column's element type is a member of the closed value set (`valuetypes()`), so untrusted schemas never
# create new method instances.

abstract type ColumnBuilder end

struct Projection
    indices::Vector{Int}       # source field positions in caller order
    slots::Vector{Int}         # source field position -> projected slot (0 means skipped)
end

# A kept field decoded by its plan `P` into `data`, whose first `len` rows are filled.
mutable struct TypedColumn{E,P<:ReadPlan} <: ColumnBuilder
    const plan::P
    data::Vector{E}
    len::Int
end

# A field that is not kept: skipped (and checked) by its concrete plan.
struct SkipColumn{P<:ReadPlan} <: ColumnBuilder
    plan::P
end

# Consecutive skipped fields fused into one cell: a run of fixed-width leaves is jumped with one bounds
# check and its values counted in bulk; otherwise each member is skipped through the closed leaf
# instruction set (`SKIP_FALLBACK` skips through the member's plan).
struct SkipRun <: ColumnBuilder
    plans::Vector{ReadPlan}
    kinds::Vector{UInt8}
    width::Int              # the run's total fixed width, or -1 when a member is not fixed-width
end

const SKIP_FALLBACK = UInt8(0)
const SKIP_LENGTH = UInt8(1)
const SKIP_DOUBLE = UInt8(2)
const SKIP_LONG = UInt8(3)
const SKIP_INT = UInt8(4)
const SKIP_BOOL = UInt8(5)
const SKIP_FLOAT = UInt8(6)
const SKIP_NULL = UInt8(7)

function skipkind(@nospecialize(p::ReadPlan))
    return p isa Union{StringPlan,BytesPlan} ? SKIP_LENGTH :
        p isa DoublePlan ? SKIP_DOUBLE :
        p isa Union{LongPlan,TimestampPlan,LocalTimestampPlan} ? SKIP_LONG :
        p isa Union{IntPlan,DatePlan} ? SKIP_INT :
        p isa BoolPlan ? SKIP_BOOL :
        p isa FloatPlan ? SKIP_FLOAT :
        p isa NullPlan ? SKIP_NULL : SKIP_FALLBACK
end

# The fixed encoded width of a skipped leaf, or -1 (varints, length-prefixed and nested values).
function skipwidth(@nospecialize(p::ReadPlan))
    return p isa NullPlan ? 0 :
        p isa FloatPlan ? 4 :
        p isa DoublePlan ? 8 :
        p isa DurationPlan ? 12 :
        p isa UUIDFixedPlan ? 16 :
        p isa FixedPlan ? p.schema.size : -1
end

# The decode cells of a record plan's builders: each run of two or more consecutive `SkipColumn`s fused
# into a `SkipRun` (`cols` itself when there is none).
function fuseskips(cols::Vector{ColumnBuilder})
    any(i -> cols[i] isa SkipColumn && cols[i + 1] isa SkipColumn, 1:length(cols) - 1) || return cols
    cells = ColumnBuilder[]
    i = 1
    while i <= length(cols)
        j = i
        while j <= length(cols) && cols[j] isa SkipColumn
            j += 1
        end
        if j - i >= 2
            plans = ReadPlan[(cols[k]::SkipColumn).plan for k in i:j - 1]
            width = 0
            for p in plans
                w = skipwidth(p)
                width = (width < 0 || w < 0) ? -1 : satadd(width, w)
            end
            push!(cells, SkipRun(plans, UInt8[skipkind(p) for p in plans], width))
            i = j
        else
            push!(cells, cols[i])
            i += 1
        end
    end
    return cells
end

# The plan producing each output field of a record plan, in field order.
function slotplans(p::RecordPlan)
    return p.fields
end

# One builder per output field: a `TypedColumn` with room for `capacity` rows for every field the
# projection keeps (`nothing` keeps all), a `SkipColumn` otherwise.
function columnbuilders(p::ReadPlan, projection::Union{Nothing,Projection}, capacity::Int)
    capacity >= 0 || throw(ArgumentError("capacity must be non-negative"))
    plans = slotplans(p)
    cols = Vector{ColumnBuilder}(undef, length(plans))
    for (i, plan) in enumerate(plans)
        if projection === nothing || projection.slots[i] != 0
            cols[i] = TypedColumn(plan, Vector{juliatype(p.schema.fields[i].schema)}(undef, capacity), 0)
        else
            cols[i] = SkipColumn(plan)
        end
    end
    return cols
end

# Builders that decode blocks straight into preallocated final columns: `finals[k]` is projected column `k`,
# and each block's rows land after the row its builders' `len` is set to.
function directbuilders(p::ReadPlan, projection::Union{Nothing,Projection}, finals::Vector{AbstractVector})
    plans = slotplans(p)
    cols = Vector{ColumnBuilder}(undef, length(plans))
    for (i, plan) in enumerate(plans)
        k = projection === nothing ? i : projection.slots[i]
        cols[i] = k == 0 ? SkipColumn(plan) : TypedColumn(plan, finals[k], 0)
    end
    return cols
end

# One record row in wire order: the record value is counted, then each cell is decoded.
function decoderow!(cells::Vector{ColumnBuilder}, d::Decoder)
    countvalue!(d)
    enter!(d)
    for c in cells
        decodecell!(c, d)
    end
    leave!(d)
    return nothing
end

function decodecell!(c::TypedColumn, d::Decoder)
    return appendcell!(c, decode(c.plan, d))
end

function decodecell!(c::SkipColumn, d::Decoder)
    return skip(c.plan, d)
end

function decodecell!(c::SkipRun, d::Decoder)
    if c.width >= 0
        countvalues!(d, length(c.plans))
        bytesavailable(d, c.width) ||
            dataerror(d, "fixed run of $(c.width) bytes exceeds the remaining $(remaining(d)) bytes")
        d.pos += c.width
        return nothing
    end
    for i in eachindex(c.kinds)
        kind = @inbounds c.kinds[i]
        if kind == SKIP_FALLBACK
            skip(@inbounds(c.plans[i]), d)
            continue
        end
        countvalue!(d)                                  # SKIP_NULL: counted, no bytes
        if kind == SKIP_LENGTH
            skiplen(d)
        elseif kind == SKIP_DOUBLE
            readdouble(d)
        elseif kind == SKIP_LONG
            readlong(d)
        elseif kind == SKIP_INT
            readint(d)
        elseif kind == SKIP_BOOL
            readbool(d)
        elseif kind == SKIP_FLOAT
            readfloat(d)
        end
    end
    return nothing
end

# Append one value, doubling the storage when it is full.
function appendcell!(c::TypedColumn, v)
    n = c.len + 1
    n > length(c.data) && resize!(c.data, max(2 * length(c.data), 4))
    @inbounds c.data[n] = v
    c.len = n
    return nothing
end

# The column trimmed to its filled rows.
function finishcolumn!(c::TypedColumn)
    return resize!(c.data, c.len)
end
