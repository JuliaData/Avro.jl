# Tables.jl integration: `Avro.Table` (a column table with a stored `Tables.Schema`), `Avro.Rows` in its
# three modes, projection pushdown through `select=`, and the read-only DataAPI metadata interface.

import DataAPI

# ---- projection -------------------------------------------------------------------------------------

"The `select=` field positions of `s` in the caller's order: one name or a collection (unknown and duplicate names are errors)."
function selectindices(s::RecordSchema, select)
    indices = Int[]
    slots = zeros(Int, length(s.fields))
    for nm in (select isa Union{Symbol,AbstractString} ? (select,) : select)
        (nm isa Symbol || nm isa AbstractString) || throw(ArgumentError("select= takes column names (Symbols or strings), got $(typeof(nm))"))
        i = get(s.fieldindex, Symbol(nm), 0)
        i == 0 && throw(SchemaError("select: record $(fullname(s)) has no field \"$nm\"", "\$"))
        slots[i] == 0 || throw(ArgumentError("select: field \"$nm\" appears more than once"))
        push!(indices, i)
        slots[i] = length(indices)
    end
    return Projection(indices, slots)
end

"""
The derived effective schema of a projection: the selected fields in the selected order with names,
types, defaults, aliases, order, docs and props unchanged.
"""
function projectschema(s::RecordSchema, sel::Vector{Int})
    # Nested values decode with every field, so a projected root substituted inside them would
    # describe them wrongly and a write of the table would drop their unselected fields.
    if length(sel) < length(s.fields)
        for i in sel
            refersto(s.fields[i].schema, s) && throw(ArgumentError(
                "select= cannot leave fields out of record $(fullname(s)): field \"$(s.fields[i].name)\" refers back to it"))
        end
    end
    return projectrecord(s, sel)
end

"Whether `root` is reachable from `x` in their schema graph."
function refersto(x::Schema, root::RecordSchema)
    return refersto(x, root, falses(graphinfo(root).nodes))
end

function refersto(x::Schema, root::RecordSchema, seen::BitVector)
    x === root && return true
    id = Int(nodeid(x)) + 1
    seen[id] && return false
    seen[id] = true
    x isa RecordSchema && return any(f -> refersto(f.schema, root, seen), x.fields)
    x isa UnionSchema && return any(b -> refersto(b, root, seen), x.branches)
    x isa ArraySchema && return refersto(x.items, root, seen)
    x isa MapSchema && return refersto(x.values, root, seen)
    return false
end

# ---- the effective schema and plan of a container operation ------------------------------------------

function effectiveplan(r::Reader, reader_schema, union_resolution::Symbol)
    reader_schema === nothing && return (r.schema, r.plan)
    plan = resolvingplan(r.schema, reader_schema; union_resolution=union_resolution, little=r.legacy === :avrojl1)
    return (reader_schema, plan)
end

# ---- Avro.Table -------------------------------------------------------------------------------------

"""
    Avro.Table(src; reader_schema=nothing, union_resolution=:spec, ntasks=Threads.nthreads(),
               limits=Limits(), legacy=nothing, allow_invalid_names=false, allow_invalid_defaults=false,
               mmap=true, select=nothing)

Materialise a container file with a record root as a Tables.jl column table (plain `Vector` columns,
a stored `Tables.Schema` without eltype parameters, DataAPI metadata).
Access columns with `table.name`, `table[:name]`, or `table[index]`; `length(table)` is the row count.
`select=` (one column name or a collection of them) is projection pushdown: unselected fields are skipped
and the result's derived schema keeps the selected fields in the selected order. Byte and memory-mapped sources decode their blocks with up to
`ntasks` tasks; the result and any error are the same at every `ntasks`.
"""
struct Table
    schema::RecordSchema
    writerschema::Schema
    names::Vector{Symbol}
    tablesschema::Tables.Schema{nothing,nothing}
    columns::Vector{AbstractVector}
    nrows::Int
    metadata::Dict{String,Vector{UInt8}}
    codecname::Symbol
    syncmarker::NTuple{16,UInt8}
end

# Block counts are claims until each block is decompressed and decoded. Final columns are preallocated
# from the claimed total only while it is no more than this floor or this multiple of the source's bytes;
# larger claims decode block by block, so a small file cannot make the reader allocate gigabytes before
# its first block is checked.
const PREALLOC_CLAIM_FLOOR = 256 * MiB
const PREALLOC_CLAIM_RATIO = 256

"Whether the claimed final columns of a pre-scanned byte source may be allocated before decoding."
function preallocatable(colstypes::Vector{Type}, nrows::Int, source::BytesSource)
    claimed = 0
    for E in colstypes
        slot = Base.elsize(Vector{E})
        slot = max(slot, 1)                                    # a column of `missing` still counts its claimed rows
        claimed = satadd(claimed, nrows > INFINITE ÷ slot ? INFINITE : slot * nrows)
    end
    datasize = source.stop - source.pos + 1
    allowed = datasize > INFINITE ÷ PREALLOC_CLAIM_RATIO ? INFINITE : PREALLOC_CLAIM_RATIO * datasize
    return claimed <= max(PREALLOC_CLAIM_FLOOR, allowed)
end

function Table(src; reader_schema::Union{Nothing,Schema}=nothing, union_resolution::Symbol=:spec,
               ntasks::Integer=Threads.nthreads(), limits::Limits=Limits(), legacy::Union{Nothing,Symbol}=nothing,
               allow_invalid_names::Bool=false, allow_invalid_defaults::Bool=false, mmap::Bool=true, select=nothing)
    1 <= ntasks <= typemax(Int) || throw(ArgumentError("ntasks must be in 1:$(typemax(Int)), got $ntasks"))
    r = Reader(src; limits=limits, legacy=legacy, allow_invalid_names=allow_invalid_names,
               allow_invalid_defaults=allow_invalid_defaults, mmap=mmap)
    try
        effective, plan = effectiveplan(r, reader_schema, union_resolution)
        effective isa RecordSchema ||
            throw(SchemaError("Avro.Table requires a record root; this source's root is $(kind(effective)): use Avro.Rows or Avro.eachdatum", "\$"))
        plan = recordrowplan(plan, effective)
        projection = select === nothing ? nothing : selectindices(effective, select)
        outschema = projection === nothing ? effective : projectschema(effective, projection.indices)
        colstypes = Type[juliatype(f.schema) for f in outschema.fields]
        pre = r.source isa BytesSource ? prescanblocks(r) : nothing
        if pre !== nothing && preallocatable(colstypes, pre.totalrows, r.source::BytesSource)
            finals = AbstractVector[Vector{E}(undef, pre.totalrows) for E in colstypes]
            decodeblocks!(r, plan, projection, finals, pre, Int(ntasks))
            nrows = pre.totalrows
        else
            finals, nrows = decodestreamed!(r, plan, projection)
        end
        return Table(outschema, r.schema, outschema.fieldnames, storedschema(outschema), finals, nrows, r.metadata, r.codecname, r.sync)
    finally
        close(r)
    end
end

function Base.length(t::Table)
    return getfield(t, :nrows)
end

function Base.show(io::IO, t::Table)
    return print(io, "Avro.Table(", getfield(t, :nrows), " rows × ", length(getfield(t, :names)), " columns: ", join(getfield(t, :names), ", "), ")")
end

function Tables.istable(::Type{Table})
    return true
end

function Tables.columnaccess(::Type{Table})
    return true
end

function Tables.columns(t::Table)
    return t
end

"A stored `Tables.Schema{nothing,nothing}`: file-derived names and eltypes never become type parameters."
function storedschema(s::RecordSchema)
    return Tables.Schema(s.fieldnames, Type[juliatype(f.schema) for f in s.fields]; stored=true)
end

function Tables.schema(t::Table)
    return getfield(t, :tablesschema)
end

function Tables.columnnames(t::Table)
    return getfield(t, :names)
end

function Tables.getcolumn(t::Table, i::Int)
    return getfield(t, :columns)[i]
end

function Tables.getcolumn(t::Table, nm::Symbol)
    i = get(getfield(t, :schema).fieldindex, nm, 0)
    i == 0 && throw(ArgumentError("no column $nm"))
    return getfield(t, :columns)[i]
end

Base.getindex(t::Table, column::Union{Int,Symbol}) = Tables.getcolumn(t, column)

function Base.getproperty(t::Table, name::Symbol)
    i = get(getfield(t, :schema).fieldindex, name, 0)
    return i == 0 ? getfield(t, name) : Tables.getcolumn(t, i)
end

function Base.propertynames(t::Table, private::Bool=false)
    columns = getfield(t, :names)
    return private ? unique(vcat(columns, collect(fieldnames(Table)))) : copy(columns)
end

function schema(t::Table)
    return getfield(t, :schema)
end

function writerschema(t::Table)
    return getfield(t, :writerschema)
end

function metadata(t::Table)
    return getfield(t, :metadata)
end

function codec(t::Table)
    return getfield(t, :codecname)
end

function sync(t::Table)
    return getfield(t, :syncmarker)
end

function DataAPI.metadatasupport(::Type{Table})
    return (read=true, write=false)
end

function DataAPI.metadatakeys(t::Table)
    return keys(getfield(t, :metadata))
end

function metadatavalue(v::Vector{UInt8})
    return isstrictutf8(v) ? String(copy(v)) : copy(v)
end

function DataAPI.metadata(t::Table, key::AbstractString; style::Bool=false)
    v = metadatavalue(getfield(t, :metadata)[String(key)])
    return style ? (v, :default) : v
end

function DataAPI.metadata(t::Table, key::AbstractString, default; style::Bool=false)
    m = getfield(t, :metadata)
    haskey(m, String(key)) || return style ? (default, :default) : default
    return DataAPI.metadata(t, key; style=style)
end

# The builders' finished columns: the kept fields in projection order.
function finishedcolumns(cols::Vector{ColumnBuilder}, projection::Union{Nothing,Projection})
    kept = projection === nothing ? eachindex(cols) : projection.indices
    return AbstractVector[finishcolumn!(cols[i]::TypedColumn) for i in kept]
end

"Every remaining block decoded into columns that grow as rows arrive, and the number of rows."
function decodestreamed!(r::Reader, plan, projection::Union{Nothing,Projection})
    d = Decoder(UInt8[], r.limits)
    cols = columnbuilders(plan, projection, 0)
    cells = rowcells(plan, cols)
    nrows = 0
    while (blk = nextblock!(r)) !== nothing
        count, bytes = blk
        reset!(d, bytes, 1)
        decoderows!(plan, cells, d, count)
        nrows += count
    end
    return (finishedcolumns(cols, projection), nrows)
end

# ---- Avro.Rows --------------------------------------------------------------------------------------

"""
    Avro.Rows(src; T=nothing, reader_schema=nothing, union_resolution=:spec, limits=Limits(),
              legacy=nothing, allow_invalid_names=false, allow_invalid_defaults=false, mmap=true,
              select=nothing)

Stream a container file's datums. **Generic record mode** (record root, no `T`): a Tables.jl row source
yielding `Avro.Record`, with one `Tables.partitions` entry per block when the schema fixes every column's
value types. **Typed mode** (`T`): a plain iterator of `T`
(no Tables interface). **Non-record mode**: a plain iterator of the generic values. `select=` applies to
the generic record mode only. `close(rows)` is idempotent; the do-block form closes on exit. Byte and
memory-mapped sources report the number of rows their block headers still claim with `length`; streamed
sources have no known length. All modes are single-pass iterators. A bad row or payload ends its block, and
iterating again continues with the next block; damaged block framing ends the stream.
"""
mutable struct Rows
    const reader::Reader
    const mode::Symbol                     # :generic, :typed or :nonrecord
    const effective::Schema
    const outschema::Union{Nothing,RecordSchema}
    const plan::Any                        # ReadPlan or TypedPlan
    const T::Any
    const select::Union{Nothing,Projection}
    const decoder::Decoder{Vector{UInt8}}  # rebound to each block
    nrows::Int                             # datums the block headers of a byte source still claim, or -1
    tablesschema::Union{Nothing,Tables.Schema{nothing,nothing}}   # built on first request
    remaining::Int                         # datums left in the current block
end

function Rows(src; T=nothing, reader_schema::Union{Nothing,Schema}=nothing, union_resolution::Symbol=:spec,
              limits::Limits=Limits(), legacy::Union{Nothing,Symbol}=nothing, allow_invalid_names::Bool=false,
              allow_invalid_defaults::Bool=false, mmap::Bool=true, select=nothing)
    r = Reader(src; limits=limits, legacy=legacy, allow_invalid_names=allow_invalid_names,
               allow_invalid_defaults=allow_invalid_defaults, mmap=mmap)
    try
        effective, plan = effectiveplan(r, reader_schema, union_resolution)
        mode = T !== nothing ? :typed : effective isa RecordSchema ? :generic : :nonrecord
        select !== nothing && mode !== :generic &&
            throw(ArgumentError("select= applies to the generic record mode only (no typed T, record root)"))
        projection = select === nothing ? nothing : selectindices(effective, select)
        outschema = mode === :generic ? (projection === nothing ? effective : projectschema(effective, projection.indices)) : nothing
        rowplan = mode === :typed ? typedplan(T, effective, plan) : mode === :generic ? recordrowplan(plan, effective) : plan
        nrows = -1
        if r.source isa BytesSource
            pre = prescanblocks(r)
            pre.pending === nothing && (nrows = pre.totalrows)   # a structural error leaves `length` unknown,
        end                                                       # as for a streamed source
        return Rows(r, mode, effective, outschema, rowplan, T, projection, Decoder(UInt8[], limits), nrows, nothing, 0)
    catch
        close(r)
        rethrow()
    end
end

function Rows(f::Function, src; kw...)
    rows = Rows(src; kw...)
    try
        return f(rows)
    finally
        close(rows)
    end
end

function Base.show(io::IO, rows::Rows)
    print(io, "Avro.Rows(")
    rows.nrows >= 0 && print(io, rows.nrows, " rows: ")
    if rows.mode === :generic
        join(io, (rows.outschema::RecordSchema).fieldnames, ", ")
    else
        print(io, rows.mode === :typed ? rows.T : kind(rows.effective))
    end
    return print(io, ")")
end

function Base.close(rows::Rows)
    rows.remaining = 0
    reset!(rows.decoder, UInt8[], 1)
    return close(rows.reader)
end

function schema(rows::Rows)
    return something(rows.outschema, rows.effective)
end

function writerschema(rows::Rows)
    return rows.reader.schema
end

function metadata(rows::Rows)
    return rows.reader.metadata
end

function codec(rows::Rows)
    return rows.reader.codecname
end

function sync(rows::Rows)
    return rows.reader.sync
end

function rowschema(rows::Rows)
    rows.mode === :generic || throw(ArgumentError("only the generic record mode has columns"))
    return rows.outschema::RecordSchema
end

function Tables.istable(rows::Rows)
    return rows.mode === :generic
end

function Tables.rowaccess(rows::Rows)
    return rows.mode === :generic
end

function Tables.rows(rows::Rows)
    return rows
end

function Tables.schema(rows::Rows)
    cached = rows.tablesschema
    cached === nothing || return cached
    value = storedschema(rowschema(rows))
    rows.tablesschema = value
    return value
end

function Tables.columnnames(rows::Rows)
    return rowschema(rows).fieldnames
end

# A row count from block headers is a claim, so `collect` and friends grow as rows decode rather than trust it.
function Base.IteratorSize(::Type{Rows})
    return Base.SizeUnknown()
end

function Base.length(rows::Rows)
    rows.nrows >= 0 || throw(ArgumentError("a streamed source has no known row count"))
    return rows.nrows
end

function consumedrows!(rows::Rows, count::Int)
    rows.nrows >= 0 && (rows.nrows -= count)
    return nothing
end

function Base.IteratorEltype(::Type{Rows})
    return Base.EltypeUnknown()
end

function Base.iterate(rows::Rows, ::Nothing=nothing)
    r = rows.reader
    checkopen(r)
    d = rows.decoder
    while rows.remaining == 0
        blk = nextblock!(r)
        blk === nothing && return nothing
        rows.remaining = blk[1]
        reset!(d, blk[2], 1)
        rows.remaining == 0 && checkconsumed(d)
    end
    left = rows.remaining - 1
    rows.remaining = 0                                 # a row that fails takes the rest of its block with it
    v = decoderow(rows, d)
    left == 0 && checkconsumed(d)
    rows.remaining = left
    consumedrows!(rows, 1)
    return (v, nothing)
end

function decoderow(rows::Rows, d::Decoder)
    plan = rows.plan
    if rows.mode === :typed
        v = decoderoot(plan, d)
        return finishtyped(plan, v)                      # semantic conversion in caller space
    end
    projection = rows.select
    projection === nothing || return Record(rows.outschema::RecordSchema, projectrow(plan, d, projection), Val(:unchecked))
    return decode(plan, d)
end

function projectrow(p::WholeRecordPlan, d::Decoder, projection::Projection)
    return getfield(decode(p.plan, d)::Record, :values)[projection.indices]
end

"Decode one record keeping the selected fields (in the caller's order); the rest are skipped."
function projectrow(p::RecordPlan, d::Decoder, projection::Projection)
    countvalue!(d)
    enter!(d)
    kept = Vector{Any}(undef, length(projection.indices))
    for (i, f) in enumerate(p.fields)
        slot = projection.slots[i]
        slot == 0 ? skip(f, d) : (kept[slot] = decode(f, d))
    end
    leave!(d)
    return kept
end

function projectrow(p::ResolvedRecordPlan, d::Decoder, projection::Projection)
    countvalue!(d)
    enter!(d)
    kept = Vector{Any}(undef, length(projection.indices))
    for (slot, plan) in p.steps
        projected = slot == 0 ? 0 : projection.slots[slot]
        projected == 0 ? skip(plan, d) : (kept[projected] = decode(plan, d))
    end
    for (slot, dp) in p.defaults
        projected = projection.slots[slot]
        projected == 0 || (kept[projected] = freshvalue(dp.value))
    end
    leave!(d)
    return kept
end

"Per-block partitions of generic-mode rows: each block materialised as an `Avro.Table` when iterated."
struct RowsPartitions
    rows::Rows
end

# Whether the schema alone fixes the types of a column's values, rather than each value choosing its own.
function typesfixed(@nospecialize(T))
    T isa Union && return all(typesfixed, Base.uniontypes(T))
    (T === Any || T === Record) && return false
    return T <: Vector ? typesfixed(eltype(T)) : T <: Dict ? typesfixed(valtype(T)) : true
end

# Sinks such as Arrow.jl choose a partition's storage from its values, and its partitions must agree. A column
# whose types vary by value could differ from block to block, so such rows stay one partition.
function Tables.partitions(rows::Rows)
    rows.mode === :generic || throw(ArgumentError("only the generic record mode has Tables partitions"))
    return all(f -> typesfixed(juliatype(f.schema)), rowschema(rows).fields) ? RowsPartitions(rows) : (rows,)
end

function Base.IteratorSize(::Type{RowsPartitions})
    return Base.SizeUnknown()
end

function Base.eltype(::Type{RowsPartitions})
    return Table
end

function Base.iterate(it::RowsPartitions, yielded::Bool=false)
    rows = it.rows
    rows.remaining == 0 || throw(ArgumentError("Tables.partitions cannot start mid-block; iterate one interface only"))
    r = rows.reader
    out = rows.outschema::RecordSchema
    blk = nextblock!(r)
    while blk !== nothing && first(blk) == 0
        blk = nextblock!(r)
    end
    if blk === nothing                              # an empty source is one empty partition, so a sink sees its columns
        yielded && return nothing
        count, bytes = 0, UInt8[]
    else
        count, bytes = blk
    end
    reset!(rows.decoder, bytes, 1)
    cols = columnbuilders(rows.plan, rows.select, min(count, length(bytes)))    # a block's count is a claim, so columns grow
    decoderows!(rows.plan, rowcells(rows.plan, cols), rows.decoder, count)
    consumedrows!(rows, count)
    t = Table(out, r.schema, out.fieldnames, storedschema(out), finishedcolumns(cols, rows.select), count, r.metadata, r.codecname, r.sync)
    return (t, true)
end

"""
Column materialisation of generic-mode rows: Tables' generic row fallback cannot allocate columns from
a stored `Tables.Schema{nothing,nothing}`, so `Rows` materialises through its own per-block column
builders (also faster). The result is an `Avro.Table` over the not-yet-iterated remainder.
"""
function Tables.columns(rows::Rows)
    out = rowschema(rows)
    rows.remaining == 0 || throw(ArgumentError("Tables.columns cannot start mid-block; iterate one interface only"))
    finals, nrows = decodestreamed!(rows.reader, rows.plan, rows.select)
    consumedrows!(rows, nrows)
    return Table(out, writerschema(rows), out.fieldnames, Tables.schema(rows), finals, nrows, metadata(rows), codec(rows), sync(rows))
end

# The stored schema keeps file-derived names out of the type system, which Tables.jl reads as a table too
# wide for `NamedTuple` rows. A caller asking for such rows (`Tables.rowtable`) opts into their types.
function Tables.namedtupleiterator(x::Union{Table,Tables.RowIterator{Table},Rows})
    return Tables.namedtupleiterator(Tables.columntable(x))
end

function retainedschema(x)
    return nothing
end

function retainedschema(t::Table)
    return getfield(t, :schema)
end

function retainedschema(rows::Rows)
    return schema(rows)
end

function retainedschema(r::Reader)
    return r.schema
end

function retainedschema(records::AbstractVector{Record})
    return isempty(records) ? nothing : uniformschema(records)
end

"The user metadata a rewrite of `x` keeps."
function retainedmetadata(x)
    return Dict{String,Vector{UInt8}}()
end

function retainedmetadata(x::Union{Table,Rows,Reader})
    return Dict{String,Vector{UInt8}}(k => v for (k, v) in metadata(x) if !startswith(k, "avro."))
end
