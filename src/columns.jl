# Column builders: a record's fields decoded straight into typed column vectors. Builders live in a
# schema-independent `Vector{ColumnBuilder}` driven by one loop that decodes each cell through a closed chain
# over the common column kinds (and a function barrier for the rest); every column's element type is a member
# of the closed value set (`valuetypes()`), so untrusted schemas never create new method instances.

abstract type ColumnBuilder end

struct Projection
    indices::Vector{Int}       # source field positions in caller order
    slots::Vector{Int}         # source field position -> projected slot (0 means skipped)
end

# A kept leaf field, or a nullable union of a leaf, decoded by its typed node `N` into `data`, whose first
# `len` rows are filled.
mutable struct NodeColumn{E,N<:TypedNode} <: ColumnBuilder
    const node::N
    data::Vector{E}
    len::Int
end

# Any other kept field, decoded generically by its plan; `nothing` when the row plan fills the column (a reader
# default, or a whole record's values).
mutable struct PlanColumn{E} <: ColumnBuilder
    const plan::Union{Nothing,Plan}
    data::Vector{E}
    len::Int
end

const TypedColumn = Union{NodeColumn,PlanColumn}

# A field that is not kept: skipped (and checked) by its plan.
struct SkipColumn <: ColumnBuilder
    plan::Plan
end

# Consecutive skipped fields fused into one cell: a run of fixed-width leaves is jumped with one bounds
# check and its values counted in bulk; otherwise each member is skipped through the closed leaf
# instruction set (`SKIP_FALLBACK` skips through the member's plan).
struct SkipRun <: ColumnBuilder
    plans::Vector{Plan}
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

function skipkind(@nospecialize(p::Plan))
    return p isa Union{StringPlan,BytesPlan} ? SKIP_LENGTH :
        p isa DoublePlan ? SKIP_DOUBLE :
        p isa Union{LongPlan,TimestampPlan,LocalTimestampPlan} ? SKIP_LONG :
        p isa Union{IntPlan,DatePlan} ? SKIP_INT :
        p isa BoolPlan ? SKIP_BOOL :
        p isa FloatPlan ? SKIP_FLOAT :
        p isa NullPlan ? SKIP_NULL : SKIP_FALLBACK
end

# The fixed encoded width of a skipped leaf, or -1 (varints, length-prefixed and nested values).
function skipwidth(@nospecialize(p::Plan))
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
    adjacent = false
    for i in 1:length(cols) - 1
        adjacent |= cols[i] isa SkipColumn && cols[i + 1] isa SkipColumn
    end
    adjacent || return cols
    cells = ColumnBuilder[]
    i = 1
    while i <= length(cols)
        j = i
        while j <= length(cols) && cols[j] isa SkipColumn
            j += 1
        end
        if j - i >= 2
            plans = Plan[(cols[k]::SkipColumn).plan for k in i:j - 1]
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

# The plan producing each output field of a record row plan, in field order (a whole record's plan for
# each of its fields).
function slotplans(@nospecialize(p::Plan))
    p isa RecordPlan && return p.fields
    p isa WholeRecordPlan && return Plan[p.plan for _ in p.schema.fields]
    plans = Vector{Plan}(undef, length(p.schema.fields))
    for (slot, plan) in (p::ResolvedRecordPlan).steps
        slot == 0 || (plans[slot] = plan)
    end
    for (slot, dp) in p.defaults
        plans[slot] = dp
    end
    return plans
end

# One builder per output field: a `TypedColumn` with room for `capacity` rows for every field the
# projection keeps (`nothing` keeps all), a `SkipColumn` otherwise.
function columnbuilders(@nospecialize(p::Plan), projection::Union{Nothing,Projection}, capacity::Int)
    capacity >= 0 || throw(ArgumentError("capacity must be non-negative"))
    fields = p.schema.fields
    return directbuilders(p, projection, AbstractVector[Vector{juliatype(fields[i].schema)}(undef, capacity)
                                                        for i in keptslots(projection, length(fields))])
end

function keptslots(projection::Union{Nothing,Projection}, n::Int)
    return projection === nothing ? (1:n) : projection.indices
end

# Builders that decode blocks straight into final columns: `finals[k]` is projected column `k`, and each
# block's rows land after the row its builders' `len` is set to.
function directbuilders(@nospecialize(p::Plan), projection::Union{Nothing,Projection}, finals::Vector{AbstractVector})
    plans = slotplans(p)
    fields = p.schema.fields
    cols = Vector{ColumnBuilder}(undef, length(plans))
    for (i, plan) in enumerate(plans)
        k = projection === nothing ? i : projection.slots[i]
        if k == 0
            cols[i] = SkipColumn(plan)
        else
            filled = p isa WholeRecordPlan || plan isa DefaultPlan       # by the row plan
            cols[i] = typedcolumn(finals[k], filled ? nothing : plan)
        end
    end
    return cols
end

function typedcolumn(data::Vector{E}, @nospecialize(plan::Union{Nothing,Plan})) where {E}
    node = plan === nothing ? nothing : columnnode(E, plan)
    node === nothing && return PlanColumn{E}(plan, data, 0)
    return NodeColumn{E,typeof(node)}(node, data, 0)
end

# The typed node of a column of a plain leaf, an array or map of one, or a nullable union whose other branch
# is a plain leaf; `nothing` for other columns, which decode generically.
function columnnode(::Type{E}, @nospecialize(p::Plan)) where {E}
    isleaftype(E) && plainleaf(p) && return LeafNode{E}(p)
    if p isa ArrayPlan
        X = eltype(E)
        return isleaftype(X) && plainleaf(p.items) ? ArrayNode{E,X}(LeafNode{X}(p.items), p.minsize) : nothing
    elseif p isa MapPlan
        V = valtype(E)
        return isleaftype(V) && plainleaf(p.values) ? MapNode{E,String,V}(LeafNode{V}(p.values), p.minsize) : nothing
    end
    E isa Union && p isa UnionPlan && p.nullable != 0 || return nothing
    X = Base.nonmissingtype(E)
    inner = p.branches[3 - p.nullable]
    (isleaftype(X) && plainleaf(inner)) || return nothing
    branches = Union{Nothing,TypedNode}[LeafNode{X}(inner), LeafNode{X}(inner)]
    branches[p.nullable] = nothing
    return NullableNode{E,X}(copy(p.branches), branches)
end

function plainleaf(@nospecialize(p::Plan))
    return !(p isa Union{RecordPlan,ArrayPlan,MapPlan,UnionPlan,WholeRecordPlan}) && !isresolving(p)
end

# One record row in wire order: the record value is counted, then each cell is decoded.
function decoderow!(cells::Vector{ColumnBuilder}, d::Decoder)
    countvalue!(d)
    enter!(d)
    for c in cells
        anycell!(c, d)
    end
    leave!(d)
    return nothing
end

# One cell through a closed chain over the common column kinds; the others dispatch at run time.
function anycell!(@nospecialize(c::ColumnBuilder), d::Decoder)
    c isa LeafColumn{Int64} && return decodecell!(c, d)
    c isa LeafColumn{String} && return decodecell!(c, d)
    c isa LeafColumn{Float64} && return decodecell!(c, d)
    c isa NullableColumn{String} && return decodecell!(c, d)
    c isa NullableColumn{Int64} && return decodecell!(c, d)
    c isa NullableColumn{Float64} && return decodecell!(c, d)
    c isa LeafColumn{Int32} && return decodecell!(c, d)
    c isa LeafColumn{Bool} && return decodecell!(c, d)
    c isa SkipColumn && return decodecell!(c, d)
    c isa SkipRun && return decodecell!(c, d)
    return decodecell!(c, d)
end

const LeafColumn{E} = NodeColumn{E,LeafNode{E}}
const NullableColumn{X} = NodeColumn{Union{Missing,X},NullableNode{Union{Missing,X},X}}

function decodecell!(c::LeafColumn{E}, d::Decoder) where {E}
    countvalue!(d)
    return appendcell!(c, leafvalue(c.node, d))
end

function decodecell!(c::NodeColumn{E,ArrayNode{E,X}}, d::Decoder) where {E,X}
    countvalue!(d)
    return appendcell!(c, arrayvalue(c.node, d))
end

function decodecell!(c::NodeColumn{E,MapNode{E,String,V}}, d::Decoder) where {E,V}
    countvalue!(d)
    return appendcell!(c, mapvalue(c.node, d))
end

# A nullable union counts its branch value as `decode` does.
function decodecell!(c::NodeColumn{E,NullableNode{E,X}}, d::Decoder) where {E,X}
    countvalue!(d)
    branches = c.node.branches
    b = branches[readindex(d, length(branches))]
    countvalue!(d)
    return appendcell!(c, b === nothing ? missing : leafvalue(b::LeafNode{X}, d))
end

function decodecell!(c::PlanColumn, d::Decoder)
    return appendcell!(c, decode(c.plan::Plan, d))
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

# ---- column builders over resolved records ------------------------------------------------------------

# A writer union resolved into a reader record has no fields of its own to decode into columns: each row
# decodes whole and its values go to the columns.
struct WholeRecordPlan <: Plan
    schema::RecordSchema
    plan::Plan
end

function recordrowplan(plan::Plan, effective::RecordSchema)
    return plan isa Union{RecordPlan,ResolvedRecordPlan} ? plan : WholeRecordPlan(effective, plan)
end

function decoderow!(cols::Vector{ColumnBuilder}, d::Decoder, p::WholeRecordPlan)
    values = getfield(decode(p.plan, d)::Record, :values)
    for (c, v) in zip(cols, values)
        c isa TypedColumn && appendcell!(c, v)
    end
    return nothing
end

# One resolved record row into its reader-slot builders (unfused): the writer-ordered steps, then the defaults.
function decoderow!(cols::Vector{ColumnBuilder}, d::Decoder, p::ResolvedRecordPlan)
    countvalue!(d)
    enter!(d)
    for (slot, plan) in p.steps
        slot == 0 ? skip(plan, d) : anycell!(cols[slot], d)
    end
    for (slot, dp) in p.defaults
        c = cols[slot]
        c isa TypedColumn && appendcell!(c, freshvalue(dp.value))
    end
    leave!(d)
    return nothing
end
