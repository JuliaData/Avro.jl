# Column builders: a record's fields decoded straight into typed column vectors. Builders live in a
# schema-independent `Vector{ColumnBuilder}` driven by one loop that decodes each cell through a closed chain
# over the common column kinds (and a function barrier for the rest); every column's element type is a member
# of the closed value set (`valuetypes()`), or `DataString` for a string column, so untrusted schemas never
# create new method instances.

abstract type ColumnBuilder end

# The element type of a generic table's column for field schema `s`. A string column is a
# `DataStrings.StringVector`, so it holds `DataString`s.
function columntype(@nospecialize(s::Schema))
    T = juliatype(s)
    T === String && return DataString
    T === Union{Missing,String} && return Union{Missing,DataString}
    return T
end

# A string column while it decodes: a payload per row, and the buffers that the payloads of strings longer than
# `DataStrings.INLINE_MAX` bytes point into. Builders add buffers under the lock.
struct StringStore{E}
    payloads::Vector{StringPayload}
    buffers::Vector{Vector{UInt8}}
    lock::ReentrantLock
end

# Where a final column of element type `E` decodes: a `Vector{E}`, or a `StringStore` for a string column.
function finalstorage(@nospecialize(E::Type), n::Int)
    (E === DataString || E === Union{Missing,DataString}) || return Vector{E}(undef, n)
    return StringStore{E}(Vector{StringPayload}(undef, n), Vector{UInt8}[], ReentrantLock())
end

# The bytes per row that `finalstorage` allocates before decoding (a long string's bytes come as it decodes).
function storagesize(@nospecialize(E::Type))
    (E === DataString || E === Union{Missing,DataString}) && return sizeof(StringPayload)
    return Base.elsize(Vector{E})
end

# A store's first `n` rows as the finished column.
function stringvector(store::StringStore{E}, n::Int) where {E}
    return StringVector{E}(resize!(store.payloads, n), store.buffers, Val(:trusted))
end

function finishedstorage(@nospecialize(x))
    return x isa StringStore ? stringvector(x, length(x.payloads)) : x::AbstractVector
end

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

# A string field. A `string` or a nullable union of one reads in place: a short string's payload holds its bytes,
# and a long one's bytes are copied into this builder's buffer, which the store gained when the builder started
# it. Other plans decode the value and append it.
mutable struct StringColumn{E} <: ColumnBuilder
    const plan::Union{Nothing,Plan}
    const inplace::Bool
    const nullable::Int                  # the null branch of an in-place nullable union, or 0
    const store::StringStore{E}
    buffer::Vector{UInt8}                # `store.buffers[bufidx + 1]`, holding `length(buffer)` bytes
    bufidx::Int                          # -1 until the first long string
    capacity::Int                        # the bytes `buffer` holds before a long string starts another
    len::Int
    last::String                         # the last value appended, and its payload, which a repeat reuses
    lastpayload::StringPayload
end

const TypedColumn = Union{NodeColumn,PlanColumn,StringColumn}

# A long string that does not fit a builder's buffer starts another, so no buffer grows by copying. Buffers
# start small, for small tables, and double up to `STRING_BUFFER_BYTES`. An Arrow view payload addresses
# its buffer with 32-bit offsets, so one string longer than that is an `ArgumentError`.
const STRING_BUFFER_START = 4 * KiB
const STRING_BUFFER_BYTES = 1 * MiB

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
    return directbuilders(p, projection, Any[finalstorage(columntype(fields[i].schema), capacity)
                                             for i in keptslots(projection, length(fields))])
end

function keptslots(projection::Union{Nothing,Projection}, n::Int)
    return projection === nothing ? (1:n) : projection.indices
end

# Builders that decode blocks straight into final columns: `finals[k]` is projected column `k`'s storage
# (`finalstorage`), and each block's rows land after the row its builders' `len` is set to.
function directbuilders(@nospecialize(p::Plan), projection::Union{Nothing,Projection}, finals::Vector{Any})
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

function typedcolumn(store::StringStore{E}, @nospecialize(plan::Union{Nothing,Plan})) where {E}
    nullable = plan isa UnionPlan && plan.nullable != 0 && plan.branches[3 - plan.nullable] isa StringPlan ? plan.nullable : 0
    return StringColumn{E}(plan, plan isa StringPlan || nullable != 0, nullable, store, UInt8[], -1, 0, 0, "",
                           DataStrings.inline_payload(UInt8[], 1, 0))
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
    c isa StringColumn{DataString} && return decodecell!(c, d)
    c isa LeafColumn{Float64} && return decodecell!(c, d)
    c isa StringColumn{Union{Missing,DataString}} && return decodecell!(c, d)
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

# Counted as `decode` counts a string and a nullable union of one.
function decodecell!(c::StringColumn, d::Decoder)
    c.inplace || return appendcell!(c, decode(c.plan::Plan, d))
    countvalue!(d)
    if c.nullable != 0
        branch = readindex(d, 2)
        countvalue!(d)
        branch == c.nullable && return appendpayload!(c, DataStrings.PAYLOAD_MISSING)
    end
    n = readlen(d)
    p = d.pos
    validutf8(d.buf, p, p + n - 1) || dataerror(d, "invalid UTF-8 in string")
    d.pos = p + n
    return appendpayload!(c, stringpayload!(c, d.buf, p, n))
end

# The payload of the `n` bytes at `p` of `src`: inline when they are short, else copied into the builder's buffer.
function stringpayload!(c::StringColumn, src::AbstractVector{UInt8}, p::Int, n::Int)
    n <= DataStrings.INLINE_MAX && return DataStrings.inline_payload(src, p, n)
    buf = c.buffer
    off = length(buf)
    if n > c.capacity - off
        capacity = max(n, clamp(2 * c.capacity, STRING_BUFFER_START, STRING_BUFFER_BYTES))
        buf = sizehint!(UInt8[], capacity)
        store = c.store
        c.bufidx = @lock store.lock (push!(store.buffers, buf); length(store.buffers) - 1)
        c.buffer = buf
        c.capacity = capacity
        off = 0
    end
    resize!(buf, off + n)                                  # within the capacity, so the bytes stay put
    copyto!(buf, off + 1, src, p, n)
    return DataStrings.view_payload(buf, off + 1, n, c.bufidx, off)
end

function appendpayload!(c::StringColumn, payload::StringPayload)
    n = c.len + 1
    payloads = c.store.payloads
    n > length(payloads) && resize!(payloads, max(2 * length(payloads), 4))
    @inbounds payloads[n] = payload
    c.len = n
    return nothing
end

# A value the row plan fills or a generic decode produced: `missing` or a `String`. A reader default fills every row
# with one value, which is stored once.
function appendcell!(c::StringColumn, v)
    v === missing && return appendpayload!(c, DataStrings.PAYLOAD_MISSING)
    s = v::String
    if s != c.last
        bytes = codeunits(s)
        c.lastpayload = stringpayload!(c, bytes, 1, length(bytes))
        c.last = s
    end
    return appendpayload!(c, c.lastpayload)
end

function finishcolumn!(c::StringColumn)
    return stringvector(c.store, c.len)
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
