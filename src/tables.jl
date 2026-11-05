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
The derived effective schema of a projection: the selected fields in the selected order, under `names` (a scan
renames or repeats columns), with types, defaults, order, docs and props unchanged. Fields keep their aliases unless
a field is renamed or repeated, which could leave two fields claiming one name.
"""
function projectschema(s::RecordSchema, sel::Vector{Int}, names::Vector{Symbol}=s.fieldnames[sel])
    reshaped = !allunique(sel) || any(k -> names[k] !== s.fieldnames[sel[k]], eachindex(sel))
    # Nested values decode with every field under its own name, so a projected root substituted inside them would
    # describe them wrongly and a write of the table would fail or misplace their fields.
    if reshaped || length(sel) < length(s.fields)
        for i in sel
            refersto(s.fields[i].schema, s) && throw(ArgumentError(
                "a read cannot leave out, repeat or rename the fields of record $(fullname(s)): field $(quoted(s.fields[i].name)) refers back to it"))
        end
    end
    fieldnames = String[]
    for (k, i) in enumerate(sel)
        if names[k] === s.fieldnames[i]
            push!(fieldnames, s.fields[i].name)
        else
            name = String(names[k])
            isvalidname(name) || throw(ArgumentError(
                "a scan cannot rename field $(quoted(s.fields[i].name)) to $(quoted(name)), which is not an Avro name"))
            push!(fieldnames, name)
        end
    end
    return projectrecord(s, sel, fieldnames; aliases=!reshaped)
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
    if x isa RecordSchema
        for f in x.fields
            refersto(f.schema, root, seen) && return true
        end
    elseif x isa UnionSchema
        for b in x.branches
            refersto(b, root, seen) && return true
        end
    end
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
    Avro.Table(src; select=nothing, filter=nothing, limit=nothing, offset=nothing, scan=nothing,
               reader_schema=nothing, union_resolution=:spec, ntasks=Threads.nthreads(), limits=Limits(),
               legacy=nothing, allow_invalid_names=false, allow_invalid_defaults=false, mmap=true)
    Avro.Table(src, T; limit=nothing, offset=nothing, kw...)

Materialise a container file with a record root as a Tables.jl column table with DataAPI metadata.
Access columns with `table.name`, `table[:name]`, or `table[index]`; `length(table)` is the row count.

**Generic** (no `T`): columns of the generic value types, string columns as `DataStrings.StringVector`s of
`DataString`s, and a stored `Tables.Schema` without eltype parameters.

* `select=` (one column name or a collection of them) keeps those columns in that order. Other fields are
  skipped, not decoded, and the result's derived schema holds the selected fields. A name the record lacks is
  an `Avro.SchemaError`.
* `filter=` keeps the rows where a predicate built with `Tables.col` is `true`, such as
  `filter=Tables.col(:fare) > 10`. Its columns are decoded whether or not they are selected.
* `offset=` skips that many of the rows the filter keeps, and `limit=` keeps at most that many. Without a
  filter, the blocks outside those rows are not decoded, and a read stops after the last of them.
* `scan=` takes a `Tables.Scan` in place of those keywords, which may also rename and repeat columns. A new
  name must be an Avro name, and a scan that renames or repeats a column leaves the fields' aliases out of the
  derived schema.

The result is what `Tables.scan` gives for the whole table, and `Tables.resolve`'s `ArgumentError` reports a
`filter=` or `scan=` that names a column the record lacks.

**Typed** (`T`, a `NamedTuple` or struct type, as for `Avro.Rows(src, T)`): each row decodes into `T` and the
table holds one `Vector{fieldtype(T, i)}` per field, named by `fieldnames(T)`, so `T` selects and orders the
columns; `limit=` and `offset=` bound the rows. Columns are type-stable, and programs compiled with
`juliac --trim=safe` read files this way. `Avro.schema(t)` is the record the rows decode from, and `Avro.write`
derives a typed table's schema from its column types.

Byte and memory-mapped sources decode their blocks with up to `ntasks` tasks, so a typed table may run `T`'s
constructor and StructUtils hooks on several tasks at once. The result and any error are the same at every
`ntasks`.
"""
struct Table{T,C}
    schema::RecordSchema
    writerschema::Schema
    columns::C                 # a generic table's `Vector{AbstractVector}`, or a typed table's tuple of vectors
    nrows::Int
    metadata::Dict{String,Vector{UInt8}}
    codecname::Symbol
    syncmarker::NTuple{16,UInt8}
end

# `T` is the row type: `Record` for a generic table.
const GenericTable = Table{Record,Vector{AbstractVector}}

function Table{T}(schema::RecordSchema, writerschema::Schema, columns::C, nrows::Int, metadata::Dict{String,Vector{UInt8}},
                  codecname::Symbol, syncmarker::NTuple{16,UInt8}) where {T,C}
    return Table{T,C}(schema, writerschema, columns, nrows, metadata, codecname, syncmarker)
end

# Block counts are claims until each block is decompressed and decoded. Final columns are preallocated
# from the claimed total only while it is no more than this floor or this multiple of the source's bytes;
# larger claims decode block by block, so a small file cannot make the reader allocate gigabytes before
# its first block is checked.
const PREALLOC_CLAIM_FLOOR = 256 * MiB
const PREALLOC_CLAIM_RATIO = 256

"Whether final columns of these element sizes may be allocated, before decoding, for the rows a pre-scanned byte source claims."
function preallocatable(elsizes, nrows::Int, source::BytesSource)
    claimed = 0
    for size in elsizes
        slot = max(size, 1)                                    # a column of `missing` still counts its claimed rows
        claimed = satadd(claimed, nrows > INFINITE ÷ slot ? INFINITE : slot * nrows)
    end
    datasize = source.stop - source.pos + 1
    allowed = datasize > INFINITE ÷ PREALLOC_CLAIM_RATIO ? INFINITE : PREALLOC_CLAIM_RATIO * datasize
    return claimed <= max(PREALLOC_CLAIM_FLOOR, allowed)
end

function tableroot(@nospecialize(s::Schema))
    s isa RecordSchema && return s
    throw(SchemaError("Avro.Table requires a record root; this source's root is $(kind(s)): use Avro.Rows or Avro.eachdatum", "\$"))
end

function Table(src; reader_schema::Union{Nothing,Schema}=nothing, union_resolution::Symbol=:spec,
               ntasks::Integer=Threads.nthreads(), limits::Limits=Limits(), legacy::Union{Nothing,Symbol}=nothing,
               allow_invalid_names::Bool=false, allow_invalid_defaults::Bool=false, mmap::Bool=true, select=nothing,
               filter=nothing, limit::Union{Nothing,Integer}=nothing, offset::Union{Nothing,Integer}=nothing,
               scan::Union{Nothing,Tables.Scan}=nothing)
    1 <= ntasks <= typemax(Int) || throw(ArgumentError("ntasks must be in 1:$(typemax(Int)), got $ntasks"))
    r = Reader(src; limits=limits, legacy=legacy, allow_invalid_names=allow_invalid_names,
               allow_invalid_defaults=allow_invalid_defaults, mmap=mmap)
    try
        effective, plan = effectiveplan(r, reader_schema, union_resolution)
        root = tableroot(effective)
        plan = recordrowplan(plan, root)
        req = tablerequest(root, scan, select, filter, rowbound(limit, "limit"), rowbound(offset, "offset"))
        if req === nothing || (req.decode == req.outputs == eachindex(root.fields) && req.names == root.fieldnames)
            projection, outschema, fields = nothing, root, eachindex(root.fields)
        else
            projection = decodeprojection(req.decode, length(root.fields))
            outschema = projectschema(root, Int[req.decode[j] for j in req.outputs], req.names)
            fields = req.decode
        end
        firstrow, lastrow = rowwindow(req)
        colstypes = Type[columntype(root.fields[i].schema) for i in fields]
        decoded, nrows, base = decodecolumns!(r, plan, projection, colstypes, firstrow, lastrow, Int(ntasks))
        columns, nrows = req === nothing ? (decoded, nrows) : keeprows(req, root, decoded, nrows, base)
        return Table{Record}(outschema, r.schema, columns, nrows, r.metadata, r.codecname, r.sync)
    finally
        close(r)
    end
end

function Table(src, ::Type{T}; reader_schema::Union{Nothing,Schema}=nothing, union_resolution::Symbol=:spec,
               ntasks::Integer=Threads.nthreads(), limits::Limits=Limits(), legacy::Union{Nothing,Symbol}=nothing,
               allow_invalid_names::Bool=false, allow_invalid_defaults::Bool=false, mmap::Bool=true,
               limit::Union{Nothing,Integer}=nothing, offset::Union{Nothing,Integer}=nothing,
               select=nothing, filter=nothing, scan=nothing) where {T}
    1 <= ntasks <= typemax(Int) || throw(ArgumentError("ntasks must be in 1:$(typemax(Int)), got $ntasks"))
    (isconcretetype(T) && recordlike(T) && T !== Record) ||
        throw(ArgumentError(string("Avro.Table(src, T) takes a concrete NamedTuple or struct type, not ", typestring(T))))
    select === nothing && filter === nothing && scan === nothing || throw(ArgumentError(
        "Avro.Table(src, T) takes its columns from the fields of T, so it has no select=, filter= or scan=; scan a generic Avro.Table(src)"))
    skip = something(rowbound(offset, "offset"), 0)
    lim = rowbound(limit, "limit")
    firstrow = satadd(skip, 1)
    lastrow = skip == typemax(Int) ? 0 : lim === nothing ? typemax(Int) : satadd(skip, lim)
    r = Reader(src; limits=limits, legacy=legacy, allow_invalid_names=allow_invalid_names,
               allow_invalid_defaults=allow_invalid_defaults, mmap=mmap)
    try
        effective, plan = effectiveplan(r, reader_schema, union_resolution)
        root = tableroot(effective)
        node = typednode(T, root, plan)
        window = nothing
        if r.source isa BytesSource
            pre, base = windowblocks(prescanblocks(r, lastrow), firstrow, lastrow)
            preallocatable(columnsizes(T), pre.totalrows, r.source::BytesSource) && (window = (pre, base))
        end
        if window !== nothing
            pre, base = window
            blocks = TypedBlocks{T}(node, typedcolumns(T, pre.totalrows))
            decodeblocks!(r, blocks, pre, Int(ntasks))
            nrows = pre.totalrows
        else
            blocks = TypedBlocks{T}(node, typedcolumns(T, 0))
            nrows, base = decodestreamed!(r, blocks, firstrow, lastrow)
        end
        rows = max(firstrow - base, 1):min(nrows, lastrow - base)
        columns = map(c -> keptrows(c, rows), blocks.columns)
        return Table{T}(root, r.schema, columns, length(rows), r.metadata, r.codecname, r.sync)
    finally
        close(r)
    end
end

# ---- scans --------------------------------------------------------------------------------------------

# What a read decodes and keeps: the record fields it decodes (each once), the decoded column behind each output
# column, the output names, a filter over the decoded columns by their source names, and the rows, counted among
# those the filter keeps, that `offset` skips and `limit` bounds.
struct TableRequest
    decode::Vector{Int}
    outputs::Vector{Int}
    names::Vector{Symbol}
    filter::Union{Nothing,Tables.BoundScan}
    offset::Int
    limit::Union{Nothing,Int}
end

"The request of a read's `scan=`, or of its `select=`, `filter=`, `limit=` and `offset=`; `nothing` for every row and column."
function tablerequest(root::RecordSchema, scan::Union{Nothing,Tables.Scan}, select, filter, limit, offset)
    if scan === nothing
        select === nothing && filter === nothing && limit === nothing && offset === nothing && return nothing
        names = select === nothing ? root.fieldnames : root.fieldnames[selectindices(root, select).indices]
        scan = Tables.Scan(select=names, filter=filter, limit=limit, offset=offset)
    elseif !(select === nothing && filter === nothing && limit === nothing && offset === nothing)
        throw(ArgumentError("pass scan= or select=, filter=, limit= and offset=, not both"))
    end
    bound = Tables.resolve(scan, root.fieldnames)
    decode = Int[]
    outputs = Int[]
    for c in bound.columns
        c.type === nothing || throw(ArgumentError(
            "Avro.Table does not convert columns, as the scan asks for $(c.name); read typed rows with Avro.Table(src, T)"))
        push!(outputs, decodeslot!(decode, c.index))
    end
    foreach(i -> decodeslot!(decode, i), bound.filtercols)
    return TableRequest(decode, outputs, Symbol[c.name for c in bound.columns], bound.filter === nothing ? nothing : bound,
                        bound.offset, bound.limit)
end

# The position of field `i` among the decoded fields, adding it when it is not there yet.
function decodeslot!(decode::Vector{Int}, i::Int)
    j = findfirst(==(i), decode)
    j === nothing || return j
    push!(decode, i)
    return length(decode)
end

function decodeprojection(decode::Vector{Int}, nfields::Int)
    slots = zeros(Int, nfields)
    for (k, i) in enumerate(decode)
        slots[i] = k
    end
    return Projection(decode, slots)
end

# A `limit=` or `offset=` as an `Int`: a negative one is an error, and one past `typemax(Int)` counts every row.
function rowbound(n::Union{Nothing,Integer}, name::String)
    n === nothing && return nothing
    n >= 0 || throw(ArgumentError("$name= must be ≥ 0, got $n"))
    return Int(min(n, typemax(Int)))
end

# The file rows a read decodes: those `offset` and `limit` bound, or every row when there is a filter to evaluate.
# No file holds a row past `typemax(Int)`, so that offset leaves none.
function rowwindow(req::Union{Nothing,TableRequest})
    (req === nothing || req.filter !== nothing) && return (1, typemax(Int))
    req.offset == typemax(Int) && return (1, 0)
    return (req.offset + 1, req.limit === nothing ? typemax(Int) : satadd(req.offset, req.limit))
end

"""
The columns `projection` decodes from the blocks that hold file rows `firstrow:lastrow`, the rows those blocks
hold, and the file rows before them.
"""
function decodecolumns!(r::Reader, plan, projection::Union{Nothing,Projection}, colstypes::Vector{Type}, firstrow::Int,
                        lastrow::Int, ntasks::Int)
    if r.source isa BytesSource
        pre, base = windowblocks(prescanblocks(r, lastrow), firstrow, lastrow)
        if preallocatable(Int[storagesize(E) for E in colstypes], pre.totalrows, r.source::BytesSource)
            storage = Any[finalstorage(E, pre.totalrows) for E in colstypes]
            decodeblocks!(r, BuilderBlocks(plan, projection, storage), pre, ntasks)
            return (AbstractVector[finishedstorage(x) for x in storage], pre.totalrows, base)
        end
    end
    return decodestreamed!(r, plan, projection, firstrow, lastrow)
end

# The columns a scan decoded, by their source names, for evaluating its filter.
struct DecodedColumns <: Tables.AbstractColumns
    names::Vector{Symbol}
    columns::Vector{AbstractVector}
    nrows::Int
end

Tables.columnnames(c::DecodedColumns) = getfield(c, :names)
Tables.getcolumn(c::DecodedColumns, i::Int) = getfield(c, :columns)[i]
Tables.getcolumn(c::DecodedColumns, nm::Symbol) = getfield(c, :columns)[something(findfirst(==(nm), getfield(c, :names)))]
Tables.rowcount(c::DecodedColumns) = getfield(c, :nrows)

"A request's output columns and their row count, from `decoded`, which holds file rows `base + 1:base + nrows`."
function keeprows(req::TableRequest, root::RecordSchema, decoded::Vector{AbstractVector}, nrows::Int, base::Int)
    if req.filter === nothing
        hi = req.limit === nothing ? nrows : min(nrows, satadd(req.offset, req.limit) - base)
        rows = max(satadd(req.offset, 1) - base, 1):hi
    else
        kept = findall(Tables.filtermask(req.filter, DecodedColumns(root.fieldnames[req.decode], decoded, nrows)))
        lo = min(req.offset, length(kept)) + 1
        hi = req.limit === nothing ? length(kept) : min(length(kept), satadd(lo - 1, req.limit))
        rows = kept[lo:hi]
    end
    used = falses(length(decoded))
    columns = AbstractVector[]
    for j in req.outputs
        column = keptrows(decoded[j], rows)
        push!(columns, used[j] ? copy(column) : column)           # a column selected twice is two columns
        used[j] = true
    end
    return (columns, length(rows))
end

# Rows `rows` of a decoded column: the column itself when they are all of it, shortened in place when they are a
# prefix of more than half of it, else a copy of just those rows.
function keptrows(column::AbstractVector, rows::AbstractVector{Int})
    rows == eachindex(column) && return column
    rows isa UnitRange && first(rows) == 1 && 2 * length(rows) > length(column) && return resize!(column, length(rows))
    return column[rows]
end

# Fewer than half a string column's rows are copied into buffers of their own, so a few kept rows do not hold the
# bytes of every decoded one.
function keptrows(column::StringVector{E}, rows::AbstractVector{Int}) where {E}
    rows == eachindex(column) && return column
    2 * length(rows) > length(column) && return StringVector{E}(column.payloads[rows], column.buffers, Val(:trusted))
    return StringVector{E}(view(column, rows))
end

# A typed table's columns for `n` rows: one `Vector{fieldtype(T, k)}` per field of `T`.
@generated function typedcolumns(::Type{T}, n::Int) where {T}
    return Expr(:tuple, (:(Vector{$(fieldtype(T, k))}(undef, n)) for k in 1:fieldcount(T))...)
end

# The element size of each of a typed table's columns.
@generated function columnsizes(::Type{T}) where {T}
    return Tuple(Base.elsize(Vector{fieldtype(T, k)}) for k in 1:fieldcount(T))
end

# Store the fields of row `v` at `row` of its table's columns.
@generated function setrow!(columns::Tuple, row::Int, v::T) where {T}
    stores = [:(columns[$k][row] = getfield(v, $k)) for k in 1:fieldcount(T)]
    return :(@inbounds begin $(stores...) end; return nothing)
end

@generated function pushrow!(columns::Tuple, v::T) where {T}
    pushes = [:(push!(columns[$k], getfield(v, $k))) for k in 1:fieldcount(T)]
    return :($(pushes...); return nothing)
end

function Base.length(t::Table)
    return getfield(t, :nrows)
end

# Tables.jl would count a table's rows by its first column, which a zero-column table lacks.
function Tables.rowcount(t::Table)
    return getfield(t, :nrows)
end

function Base.show(io::IO, t::Table)
    names = Tables.columnnames(t)
    return print(io, "Avro.Table(", getfield(t, :nrows), " rows × ", length(names), " columns: ", join(names, ", "), ")")
end

function Tables.istable(::Type{<:Table})
    return true
end

function Tables.columnaccess(::Type{<:Table})
    return true
end

function Tables.columns(t::Table)
    return t
end

"""
A stored `Tables.Schema{nothing,nothing}` of the element types `eltypeof` gives each field: file-derived names and
eltypes never become type parameters.
"""
function storedschema(s::RecordSchema, eltypeof=juliatype)
    return Tables.Schema(s.fieldnames, Type[eltypeof(f.schema) for f in s.fields]; stored=true)
end

function Tables.schema(t::GenericTable)
    return storedschema(getfield(t, :schema), columntype)
end

# A typed table's names and eltypes are its row type's.
function Tables.schema(::Table{T,<:Tuple}) where {T}
    return Tables.Schema{fieldnames(T),Tuple{fieldtypes(T)...}}()
end

function Tables.columnnames(t::GenericTable)
    return getfield(t, :schema).fieldnames
end

function Tables.columnnames(::Table{T,<:Tuple}) where {T}
    return fieldnames(T)
end

# The position of column `name`, or 0.
function columnindex(t::GenericTable, name::Symbol)
    return get(getfield(t, :schema).fieldindex, name, 0)
end

function columnindex(::Table{T,<:Tuple}, name::Symbol) where {T}
    return Base.fieldindex(T, name, false)
end

function Tables.getcolumn(t::Table, i::Int)
    return getfield(t, :columns)[i]
end

function Tables.getcolumn(t::Table, nm::Symbol)
    i = columnindex(t, nm)
    i == 0 && throw(ArgumentError("no column $nm"))
    return getfield(t, :columns)[i]
end

Base.getindex(t::Table, column::Union{Int,Symbol}) = Tables.getcolumn(t, column)

function Base.getproperty(t::Table, name::Symbol)
    i = columnindex(t, name)
    return i == 0 ? getfield(t, name) : getfield(t, :columns)[i]
end

function Base.propertynames(t::Table, private::Bool=false)
    columns = collect(Symbol, Tables.columnnames(t))
    return private ? unique(vcat(columns, collect(fieldnames(typeof(t))))) : columns
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

function DataAPI.metadatasupport(::Type{<:Table})
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

"""
The remaining blocks from the one holding file row `firstrow` to the one holding `lastrow`, decoded into columns that
grow as rows arrive: the columns, the rows they hold, and the file rows before them. Earlier blocks are not
decompressed, and later ones are not read.
"""
function decodestreamed!(r::Reader, plan, projection::Union{Nothing,Projection}, firstrow::Int=1, lastrow::Int=typemax(Int))
    d = Decoder(UInt8[], r.limits)
    cols = columnbuilders(plan, projection, 0)
    cells = rowcells(plan, cols)
    nrows = 0
    base, count = firstblock!(r, d, firstrow, lastrow)
    while count !== nothing
        decoderows!(plan, cells, d, count)
        nrows = satadd(nrows, count)
        satadd(base, nrows) < lastrow || break
        count = nextblock!(r, d)
    end
    return (finishedcolumns(cols, projection), nrows, base)
end

"As for a generic table, each row decoded into `T` and its fields pushed onto the columns: the rows and the file rows before them."
function decodestreamed!(r::Reader, b::TypedBlocks{T}, firstrow::Int=1, lastrow::Int=typemax(Int)) where {T}
    d = Decoder(UInt8[], r.limits)
    nrows = 0
    base, count = firstblock!(r, d, firstrow, lastrow)
    while count !== nothing
        for _ in 1:count
            pushrow!(b.columns, decodenode(T, b.node, d))
        end
        checkconsumed(d)
        nrows = satadd(nrows, count)
        satadd(base, nrows) < lastrow || break
        count = nextblock!(r, d)
    end
    return (nrows, base)
end

# The file rows before the first block a streamed read of rows `firstrow:lastrow` decodes, and that block's count with
# the block loaded into `d`; no block for an empty range.
function firstblock!(r::Reader, d::Decoder{Vector{UInt8}}, firstrow::Int, lastrow::Int)
    lastrow < firstrow && return (0, nothing)
    firstrow == 1 && return (0, nextblock!(r, d))
    return nextblockfrom!(r, d, firstrow)
end

# ---- Avro.Rows --------------------------------------------------------------------------------------

"""
    Avro.Rows(src; reader_schema=nothing, union_resolution=:spec, limits=Limits(), legacy=nothing,
              allow_invalid_names=false, allow_invalid_defaults=false, mmap=true, select=nothing)
    Avro.Rows(src, T; kw...)

Stream a container file's datums. **Generic record mode** (record root, no `T`): a Tables.jl row source
yielding `Avro.Record`, with one `Tables.partitions` entry per block when the schema fixes every column's
value types. **Typed mode** (`T`): a plain iterator of `T` (no Tables interface). **Non-record mode**: a
plain iterator of the generic values. `select=` applies to the generic record mode only. `close(rows)` is
idempotent; the do-block form closes on exit. Byte and memory-mapped sources report the number of rows
their block headers still claim with `length`; streamed sources have no known length. All modes are
single-pass iterators. A bad row or payload ends its block, and iterating again continues with the next
block; damaged block framing ends the stream.
"""
mutable struct Rows{T}
    const reader::Reader
    const mode::Symbol                     # :generic, :typed or :nonrecord
    const effective::Schema
    const outschema::Union{Nothing,RecordSchema}
    const plan::Plan                       # the row plan of the generic record mode's projections and columns
    const node::TypedNode                  # decodes a whole row into `T`
    const select::Union{Nothing,Projection}
    const decoder::Decoder{Vector{UInt8}}  # rebound to each block
    nrows::Int                             # datums the block headers of a byte source still claim, or -1
    tablesschema::Union{Nothing,Tables.Schema{nothing,nothing}}   # built on first request
    remaining::Int                         # datums left in the current block
end

const RowsSource = Union{AbstractString,IO,AbstractVector{UInt8}}

function Rows(src::RowsSource, ::Type{T}; reader_schema::Union{Nothing,Schema}=nothing, union_resolution::Symbol=:spec,
              limits::Limits=Limits(), legacy::Union{Nothing,Symbol}=nothing, allow_invalid_names::Bool=false,
              allow_invalid_defaults::Bool=false, mmap::Bool=true) where {T}
    r = Reader(src; limits=limits, legacy=legacy, allow_invalid_names=allow_invalid_names,
               allow_invalid_defaults=allow_invalid_defaults, mmap=mmap)
    try
        effective, plan = effectiveplan(r, reader_schema, union_resolution)
        node = typednode(T, effective, plan)
        return Rows{T}(r, :typed, effective, nothing, plan, node, nothing, Decoder(UInt8[], limits), claimedrows(r), nothing, 0)
    catch
        close(r)
        rethrow()
    end
end

function Rows(src; reader_schema::Union{Nothing,Schema}=nothing, union_resolution::Symbol=:spec, limits::Limits=Limits(),
              legacy::Union{Nothing,Symbol}=nothing, allow_invalid_names::Bool=false, allow_invalid_defaults::Bool=false,
              mmap::Bool=true, select=nothing)
    r = Reader(src; limits=limits, legacy=legacy, allow_invalid_names=allow_invalid_names,
               allow_invalid_defaults=allow_invalid_defaults, mmap=mmap)
    try
        effective, plan = effectiveplan(r, reader_schema, union_resolution)
        decoder = Decoder(UInt8[], limits)
        if !(effective isa RecordSchema)
            select === nothing || throw(ArgumentError("select= applies to the generic record mode only (no typed T, record root)"))
            return Rows{Any}(r, :nonrecord, effective, nothing, plan, GenericNode{Any}(plan), nothing, decoder, claimedrows(r), nothing, 0)
        end
        projection = select === nothing ? nothing : selectindices(effective, select)
        outschema = projection === nothing ? effective : projectschema(effective, projection.indices)
        return Rows{Record}(r, :generic, effective, outschema, recordrowplan(plan, effective), GenericNode{Record}(plan),
                            projection, decoder, claimedrows(r), nothing, 0)
    catch
        close(r)
        rethrow()
    end
end

# The datums the block headers of a byte source claim, or -1 (a streamed source, or a structural error
# that leaves `length` unknown as for a streamed source).
function claimedrows(r::Reader)
    r.source isa BytesSource || return -1
    pre = prescanblocks(r)
    return pre.pending === nothing ? pre.totalrows : -1
end

function Rows(f::Function, src, args...; kw...)
    rows = Rows(src, args...; kw...)
    try
        return f(rows)
    finally
        close(rows)
    end
end

function Base.show(io::IO, rows::Rows{T}) where {T}
    print(io, "Avro.Rows(")
    rows.nrows >= 0 && print(io, rows.nrows, " rows: ")
    if rows.mode === :generic
        join(io, (rows.outschema::RecordSchema).fieldnames, ", ")
    else
        print(io, rows.mode === :typed ? T : kind(rows.effective))
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
function Base.IteratorSize(::Type{<:Rows})
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

function Base.IteratorEltype(::Type{<:Rows})
    return Base.EltypeUnknown()
end

function Base.iterate(rows::Rows, ::Nothing=nothing)
    v = nextdatum!(d -> decoderow(rows, d), rows)
    v === nothing && return nothing
    consumedrows!(rows, 1)
    return (something(v), nothing)
end

function decoderow(rows::Rows{T}, d::Decoder) where {T}
    if T === Record
        projection = rows.select
        projection === nothing ||
            return Record(rows.outschema::RecordSchema, projectrow(rows.plan, d, projection), Val(:unchecked))
    end
    return decodenode(T, rows.node, d)
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
    T isa Union && return typesfixed(T.a) && typesfixed(T.b)
    (T === Any || T === Record) && return false
    return T <: Vector ? typesfixed(eltype(T)) : T <: Dict ? typesfixed(valtype(T)) : true
end

# Sinks such as Arrow.jl choose a partition's storage from its values, and its partitions must agree. A column
# whose types vary by value could differ from block to block, so such rows stay one partition.
function Tables.partitions(rows::Rows)
    rows.mode === :generic || throw(ArgumentError("only the generic record mode has Tables partitions"))
    for f in rowschema(rows).fields
        typesfixed(juliatype(f.schema)) || return (rows,)
    end
    return RowsPartitions(rows)
end

function Base.IteratorSize(::Type{RowsPartitions})
    return Base.SizeUnknown()
end

function Base.eltype(::Type{RowsPartitions})
    return GenericTable
end

function Base.iterate(it::RowsPartitions, yielded::Bool=false)
    rows = it.rows
    rows.remaining == 0 || throw(ArgumentError("Tables.partitions cannot start mid-block; iterate one interface only"))
    r = rows.reader
    out = rows.outschema::RecordSchema
    d = rows.decoder
    count = nextblock!(r, d)
    while count == 0
        count = nextblock!(r, d)
    end
    if count === nothing                            # an empty source is one empty partition, so a sink sees its columns
        yielded && return nothing
        count = 0
        blockcap!(reset!(d, UInt8[], 1))
    end
    cols = columnbuilders(rows.plan, rows.select, min(count, remaining(d)))    # a block's count is a claim, so columns grow
    decoderows!(rows.plan, rowcells(rows.plan, cols), rows.decoder, count)
    consumedrows!(rows, count)
    t = Table{Record}(out, r.schema, finishedcolumns(cols, rows.select), count, r.metadata, r.codecname, r.sync)
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
    finals, nrows, _ = decodestreamed!(rows.reader, rows.plan, rows.select)
    consumedrows!(rows, nrows)
    return Table{Record}(out, writerschema(rows), finals, nrows, metadata(rows), codec(rows), sync(rows))
end

# The stored schema keeps file-derived names out of the type system, which Tables.jl reads as a table too
# wide for `NamedTuple` rows. A caller asking for such rows (`Tables.rowtable`) opts into their types.
function Tables.namedtupleiterator(x::Union{GenericTable,Tables.RowIterator{GenericTable},Rows})
    return Tables.namedtupleiterator(Tables.columntable(x))
end

function retainedschema(x)
    return nothing
end

function retainedschema(t::GenericTable)
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
