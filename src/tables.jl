# Tables.jl integration (plan §6): `Avro.Table` (a column table with a stored `Tables.Schema`),
# `Avro.Rows` in its three modes, `Avro.Row` (a record plus the operation's admission object),
# projection pushdown through `select=`, and the read-only DataAPI metadata interface. Symbol admission
# is the trust boundary: column names intern only through the operation's admission object, lazily for
# `Rows`.

import DataAPI

# ---- Avro.Row ---------------------------------------------------------------------------------------

"""
    Avro.Row

The row type `Avro.Rows` yields in generic record mode: the decoded `Avro.Record` together with the
operation's symbol-admission object, so every `Symbol`-producing path honours the caller's boundary.
`Avro.Row(record; names=Avro.DEFAULT_ADMISSION)` wraps a detached record.
"""
struct Row <: Tables.AbstractRow
    record::Record
    admission::Union{SymbolAdmission,Symbol}
end

Row(record::Record; names=DEFAULT_ADMISSION) = Row(record, admission(names))

function admitnames(s::RecordSchema, adm, budget::Union{Nothing,Budget}=nothing)
    return Symbol[admit!(adm, f.name; budget=budget) for f in s.fields]
end

Tables.columnnames(r::Row) = admitnames(getfield(getfield(r, :record), :schema), getfield(r, :admission))
Tables.getcolumn(r::Row, i::Int) = getfield(getfield(r, :record), :values)[i]
Tables.getcolumn(r::Row, nm::Symbol) = getfield(r, :record)[String(nm)]
Base.show(io::IO, r::Row) = (print(io, "Avro.Row"); show(io, getfield(r, :record)))

"The wrapped `Avro.Record` of a row."
record(r::Row) = getfield(r, :record)

# ---- projection -------------------------------------------------------------------------------------

"The `select=` field positions of `s` in the caller's order (unknown and duplicate names are errors)."
function selectindices(s::RecordSchema, select)
    sel = Int[]
    for nm in select
        (nm isa Symbol || nm isa AbstractString) || throw(ArgumentError("select= takes column names (Symbols or strings), got $(typeof(nm))"))
        name = String(nm)
        i = get(s.fieldindex, name, 0)
        i == 0 && throw(ArgumentError("select: record $(fullname(s)) has no field \"$(escapename(name))\""))
        i in sel && throw(ArgumentError("select: duplicate field \"$(escapename(name))\""))
        push!(sel, i)
    end
    return sel
end

"""
The derived effective schema of a projection: the selected fields in the selected order with names,
types, defaults, aliases, order, docs and props unchanged; a recursive root is substituted graph-wide.
"""
function projectschema(s::RecordSchema, sel::Vector{Int}, limits::Limits)
    memo = FrozenDict{String,Schema}()
    fields = FrozenVector{Field}()
    index = FrozenDict{String,Int}()
    out = RecordSchema(s.name, s.aliases, s.rawaliases, s.doc, s.iserror, s.props, fields, index, NodeMeta())
    memo[fullname(s)] = out
    for (k, i) in enumerate(sel)
        f = s.fields[i]
        push!(fields, Field(f.name, deepcopyschema(f.schema, memo), f.doc, f.default, f.order, f.aliases, f.props))
        index[f.name] = k
    end
    freeze!(fields)
    freeze!(index)
    return finalizepublic!(out, limits, 0, 0)
end

# ---- the effective schema and plan of a container operation ------------------------------------------

function effectiveplan(r::Reader, reader_schema, union_resolution::Symbol, limits::Limits, decimal_byteorder::Symbol)
    reader_schema === nothing && return (r.schema, r.plan)
    plan = resolvingplan(r.schema, reader_schema; union_resolution=union_resolution, limits=limits,
                         budget=r.budget)
    decimal_byteorder === :little && (plan = littledecimals(plan))
    return (reader_schema, plan)
end

# ---- Avro.Table -------------------------------------------------------------------------------------

"""
    Avro.Table(src; reader_schema=nothing, union_resolution=:spec, ntasks=Threads.nthreads(),
               limits=Limits(), legacy=nothing, decimal_byteorder=:big, allow_invalid_names=false,
               allow_invalid_defaults=false, validate=:strict, mmap=true, names=Avro.DEFAULT_ADMISSION,
               select=nothing)

Materialise a container file with a record root as a Tables.jl column table (plain `Vector` columns,
a stored `Tables.Schema` without eltype parameters, `Tables.partitions` per block, DataAPI metadata).
`select=` is projection pushdown: unselected fields are skipped under the active validation mode and
the result's derived schema keeps the selected fields in the selected order.
"""
struct Table
    schema::RecordSchema
    writerschema::Schema
    names::Vector{Symbol}
    columns::Vector{AbstractVector}
    nrows::Int
    blockranges::Vector{UnitRange{Int}}
    metadata::Map{Vector{UInt8}}
    codecname::Symbol
    syncmarker::NTuple{16,UInt8}
end

function Table(src; reader_schema::Union{Nothing,Schema}=nothing, union_resolution::Symbol=:spec,
               ntasks::Integer=Threads.nthreads(), limits::Limits=Limits(), legacy::Union{Nothing,Symbol}=nothing,
               decimal_byteorder::Symbol=:big, allow_invalid_names::Bool=false, allow_invalid_defaults::Bool=false,
               validate::Symbol=:strict, mmap::Bool=true, names=DEFAULT_ADMISSION, select=nothing)
    1 <= ntasks <= typemax(Int) || throw(ArgumentError("ntasks must be in 1:$(typemax(Int)), got $ntasks"))
    taskcount = Int(ntasks)
    adm = admission(names)
    r = Reader(src; limits=limits, legacy=legacy, decimal_byteorder=decimal_byteorder, allow_invalid_names=allow_invalid_names,
               allow_invalid_defaults=allow_invalid_defaults, validate=validate, mmap=mmap)
    try
        effective, plan = effectiveplan(r, reader_schema, union_resolution, limits, decimal_byteorder)
        effective isa RecordSchema ||
            throw(ArgumentError("Avro.Table requires a record root; this source's root is $(kind(effective)) — use Avro.Rows or Avro.eachdatum"))
        sel = select === nothing ? nothing : selectindices(effective, select)
        outschema = sel === nothing ? effective : projectschema(effective, sel, limits)
        keptidx = sel === nothing ? collect(1:length(effective.fields)) : sel
        colstypes = Type[juliatype(f.schema) for f in outschema.fields]
        if r.source isa BytesSource
            # byte and mapped sources: pre-scan, exact final preallocation, direct/parallel decode (§4.9)
            pre = prescanblocks(r)
            nrows = pre.totalrows
            finals = AbstractVector[]
            for E in colstypes
                reserve!(r.budget, vectorbytes(E, nrows))
                push!(finals, Vector{E}(undef, nrows))
                allocated!(r.budget, vectorbytes(E, nrows))
            end
            decodeblocks!(r, plan, sel, finals, keptidx, colstypes, pre, taskcount)
            counts = Int[e.count for e in pre.entries]
        else
            finals, counts = decodestreamed!(r, plan, sel, colstypes)
        end
        nrows = sum(counts; init=0)
        ranges = UnitRange{Int}[]
        off = 0
        for c in counts
            push!(ranges, off + 1:off + c)
            off += c
        end
        outnames = admitnames(outschema, adm, r.budget)
        return Table(outschema, r.schema, outnames, finals, nrows, ranges, r.metadata, r.codecname, r.sync)
    finally
        close(r)
    end
end

Base.length(t::Table) = getfield(t, :nrows)
Base.show(io::IO, t::Table) = print(io, "Avro.Table(", getfield(t, :nrows), " rows × ", length(getfield(t, :names)), " columns: ", join(getfield(t, :names), ", "), ")")

Tables.istable(::Type{Table}) = true
Tables.columnaccess(::Type{Table}) = true
Tables.columns(t::Table) = t
"A stored `Tables.Schema{nothing,nothing}`: file-derived names and eltypes never become type parameters."
storedschema(names::Vector{Symbol}, s::RecordSchema) = Tables.Schema(names, Type[juliatype(f.schema) for f in s.fields]; stored=true)

Tables.schema(t::Table) = storedschema(getfield(t, :names), getfield(t, :schema))
Tables.columnnames(t::Table) = getfield(t, :names)
Tables.getcolumn(t::Table, i::Int) = getfield(t, :columns)[i]
function Tables.getcolumn(t::Table, nm::Symbol)
    i = findfirst(==(nm), getfield(t, :names))
    i === nothing && throw(ArgumentError("no column $nm"))
    return getfield(t, :columns)[i]
end
Tables.partitions(t::Table) = (subtable(t, r) for r in getfield(t, :blockranges))

# Tables' generic column-to-row fallback derives the row count from the first column, so it yields no
# rows for a zero-column table. Keep Table's column-access contract, but provide a row view that uses
# the authoritative `nrows` field. This preserves row counts when `select=()` is written again.
struct _TableRows
    table::Table
end

struct _TableRow <: Tables.AbstractRow
    table::Table
    index::Int
end

function Tables.rows(t::Table)
    return _TableRows(t)
end

function Tables.isrowtable(::Type{_TableRows})
    return true
end

function Tables.schema(rows::_TableRows)
    return Tables.schema(getfield(rows, :table))
end

function Tables.columnnames(rows::_TableRows)
    return Tables.columnnames(getfield(rows, :table))
end

function Base.IteratorSize(::Type{_TableRows})
    return Base.HasLength()
end

function Base.eltype(::Type{_TableRows})
    return _TableRow
end

function Base.length(rows::_TableRows)
    return length(getfield(rows, :table))
end

function Base.iterate(rows::_TableRows, index::Int=1)
    table = getfield(rows, :table)
    index > length(table) && return nothing
    return (_TableRow(table, index), index + 1)
end

function Tables.columnnames(row::_TableRow)
    return Tables.columnnames(getfield(row, :table))
end

function Tables.getcolumn(row::_TableRow, column::Int)
    return Tables.getcolumn(getfield(row, :table), column)[getfield(row, :index)]
end

function Tables.getcolumn(row::_TableRow, name::Symbol)
    return Tables.getcolumn(getfield(row, :table), name)[getfield(row, :index)]
end

function subtable(t::Table, range::UnitRange{Int})
    return Table(getfield(t, :schema), getfield(t, :writerschema), getfield(t, :names),
                 AbstractVector[view(c, range) for c in getfield(t, :columns)], length(range), [1:length(range)],
                 getfield(t, :metadata), getfield(t, :codecname), getfield(t, :syncmarker))
end

schema(t::Table) = getfield(t, :schema)
writerschema(t::Table) = getfield(t, :writerschema)
metadata(t::Table) = getfield(t, :metadata)
codec(t::Table) = getfield(t, :codecname)
sync(t::Table) = getfield(t, :syncmarker)

DataAPI.metadatasupport(::Type{Table}) = (read=true, write=false)
DataAPI.metadatakeys(t::Table) = (k for k in getfield(t, :metadata).keys)

function metadatavalue(v::Vector{UInt8})
    return validutf8(v, 1, length(v)) ? String(copy(v)) : copy(v)
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

"The streamed consumer: per-block exact chunk columns assembled once at the end (plan §4.4/§4.9)."
function decodestreamed!(r::Reader, plan, sel::Union{Nothing,Vector{Int}}, colstypes::Vector{Type})
    reserve!(r.budget, blocktablecharge(0))
    counts = Int[]
    chunkcols = Vector{AbstractVector}[]
    allocated!(r.budget, blocktablecharge(0))
    slotrow = sum(slotbytes, colstypes; init=0)
    while (blk = nextblock!(r; walk=false)) !== nothing
        reserve!(r.budget, 40 + STORAGE[].vector + 8 * length(colstypes))   # the block-table entry and outer chunk container (R06)
        count, bytes = blk
        reserve!(r.budget, bytesbytes(length(bytes)))
        allocated!(r.budget, bytesbytes(length(bytes)))                     # decompressed by nextblock!; already resident
        d = Decoder(bytes, r.budget; validate=r.validate)
        cols = columnbuilders(plan, sel, count, r.budget)
        cells = plan isa RecordPlan ? fuseskips(cols) : cols
        outputbase = r.budget.reserved
        cap = r.limits.max_block_output_bytes
        for done in 1:count
            countvalues!(r.budget)
            decoderow!(cells, d, plan)
            checkblockoutput(r.budget, outputbase, done, slotrow, cap)
        end
        d.pos == length(bytes) + 1 || throw(DataError("block datums did not consume the block exactly", d.pos))
        release!(r.budget, bytesbytes(length(bytes)))
        keep = AbstractVector[]
        for i in (sel === nothing ? eachindex(cols) : sel)
            push!(keep, finishcolumn!(cols[i]::TypedColumn, r.budget))
        end
        push!(chunkcols, keep)
        push!(counts, count)
        allocated!(r.budget, 40 + STORAGE[].vector + 8 * length(colstypes))
    end
    nrows = sum(counts; init=0)
    finals = AbstractVector[]
    for E in colstypes                                 # both sets of reference slots coexist during assembly (plan §4.4)
        reserve!(r.budget, vectorbytes(E, nrows))
        push!(finals, Vector{E}(undef, nrows))
        allocated!(r.budget, vectorbytes(E, nrows))
    end
    for k in eachindex(finals)
        col = finals[k]
        off = 0
        for chunk in chunkcols
            c = chunk[k]
            copyto!(col, off + 1, c, 1, length(c))
            off += length(c)
        end
    end
    for chunk in chunkcols
        for c in chunk
            release!(r.budget, vectorbytes(eltype(c), length(c)))   # referenced payload transfers, counted once
        end
    end
    return (finals, counts)
end

# ---- Avro.Rows --------------------------------------------------------------------------------------

"""
    Avro.Rows(src; T=nothing, reader_schema=nothing, union_resolution=:spec, limits=Limits(),
              legacy=nothing, decimal_byteorder=:big, allow_invalid_names=false,
              allow_invalid_defaults=false, validate=:strict, mmap=true, names=Avro.DEFAULT_ADMISSION,
              select=nothing)

Stream a container file's datums. **Generic record mode** (record root, no `T`): a Tables.jl row source
yielding `Avro.Row`, with lazy column-name admission and per-block `Tables.partitions`. **Typed mode**
(`T`): a plain iterator of `T` (no Tables interface). **Non-record mode**: a plain iterator of the §4.6
values. `select=` applies to the generic record mode only. `close(rows)` is idempotent; the do-block
form closes on exit.
"""
mutable struct Rows{L}                     # L::Bool — a byte-source pre-scan supplied an exact length (R09)
    const reader::Reader
    const mode::Symbol                     # :generic, :typed or :nonrecord
    const effective::Schema
    const outschema::Union{Nothing,RecordSchema}
    const plan::Any                        # ReadPlan or TypedPlan
    const T::Any
    const adm::Union{SymbolAdmission,Symbol}
    const select::Union{Nothing,Vector{Int}}
    const nrows::Int                       # exact datum count when L (0 otherwise)
    symbols::Union{Nothing,Vector{Symbol}} # lazy
    bytes::Vector{UInt8}
    decoder::Union{Nothing,Decoder{Vector{UInt8}}}
    remaining::Int
    lastcharge::Int
    blockout::Int
end

function Rows(src; T=nothing, reader_schema::Union{Nothing,Schema}=nothing, union_resolution::Symbol=:spec,
              limits::Limits=Limits(), legacy::Union{Nothing,Symbol}=nothing, decimal_byteorder::Symbol=:big,
              allow_invalid_names::Bool=false, allow_invalid_defaults::Bool=false, validate::Symbol=:strict,
              mmap::Bool=true, names=DEFAULT_ADMISSION, select=nothing)
    adm = admission(names)
    r = Reader(src; limits=limits, legacy=legacy, decimal_byteorder=decimal_byteorder, allow_invalid_names=allow_invalid_names,
               allow_invalid_defaults=allow_invalid_defaults, validate=validate, mmap=mmap)
    try
        effective, plan = effectiveplan(r, reader_schema, union_resolution, limits, decimal_byteorder)
        mode = T !== nothing ? :typed : effective isa RecordSchema ? :generic : :nonrecord
        select !== nothing && mode !== :generic &&
            throw(ArgumentError("select= applies to the generic record mode only (no typed T, record root)"))
        sel = select === nothing ? nothing : selectindices(effective, select)
        outschema = mode === :generic ? (sel === nothing ? effective : projectschema(effective, sel, limits)) : nothing
        rowplan = mode === :typed ? typedplan(T, effective, plan, limits; budget=r.budget) : plan
        nrows = -1
        if r.source isa BytesSource
            pre = prescanblocks(r)                      # headers only; the table charge is transient
            release!(r.budget, blocktablecharge(max(64, nextpow2rows(length(pre.entries)))))
            pre.pending === nothing && (nrows = pre.totalrows)   # a structural error keeps SizeUnknown
        end                                                       # so acceptance matches streamed sources
        nrows >= 0 && return Rows{true}(r, mode, effective, outschema, rowplan, T, adm, sel, nrows, nothing, UInt8[], nothing, 0, 0, 0)
        return Rows{false}(r, mode, effective, outschema, rowplan, T, adm, sel, 0, nothing, UInt8[], nothing, 0, 0, 0)
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

Base.close(rows::Rows) = close(getfield(rows, :reader))

schema(rows::Rows) = something(getfield(rows, :outschema), getfield(rows, :effective))
writerschema(rows::Rows) = getfield(rows, :reader).schema
metadata(rows::Rows) = getfield(rows, :reader).metadata
codec(rows::Rows) = getfield(rows, :reader).codecname
sync(rows::Rows) = getfield(rows, :reader).sync

function rowsymbols(rows::Rows)
    getfield(rows, :mode) === :generic || throw(ArgumentError("only the generic record mode has columns"))
    s = getfield(rows, :symbols)
    s === nothing || return s
    s = admitnames(getfield(rows, :outschema), getfield(rows, :adm), getfield(rows, :reader).budget)   # names admit lazily, on first request
    setfield!(rows, :symbols, s)
    return s
end

Tables.istable(rows::Rows) = getfield(rows, :mode) === :generic
Tables.rowaccess(rows::Rows) = getfield(rows, :mode) === :generic
Tables.rows(rows::Rows) = rows
Tables.schema(rows::Rows) = storedschema(rowsymbols(rows), getfield(rows, :outschema))
Tables.columnnames(rows::Rows) = rowsymbols(rows)

"The block-table capacity the pre-scan grew to for `n` entries (its growth doubles from 64)."
function nextpow2rows(n::Int)
    cap = 64
    while cap < n
        cap *= 2
    end
    return cap
end

function Base.IteratorSize(::Type{<:Rows})
    return Base.SizeUnknown()
end

function Base.IteratorSize(::Type{Rows{true}})
    return Base.HasLength()
end

function Base.length(rows::Rows{true})
    return getfield(rows, :nrows)
end

function Base.IteratorEltype(::Type{<:Rows})
    return Base.EltypeUnknown()
end

function Base.iterate(rows::Rows, ::Nothing=nothing)
    r = rows.reader
    b = r.budget
    release!(b, rows.lastcharge)
    rows.lastcharge = 0
    while rows.remaining == 0
        blk = nextblock!(r; walk=false)
        blk === nothing && return nothing
        rows.remaining = blk[1]
        rows.bytes = blk[2]
        rows.blockout = 0
        reserve!(b, bytesbytes(length(rows.bytes)))
        allocated!(b, bytesbytes(length(rows.bytes)))          # decompressed by nextblock!; already resident
        rows.decoder = Decoder(rows.bytes, b; validate=r.validate)
        rows.remaining == 0 && release!(b, bytesbytes(length(rows.bytes)))
    end
    rows.remaining -= 1
    before = b.reserved
    v = decoderow(rows)
    rows.lastcharge = max(b.reserved - before, 0)
    rows.blockout = checked_add(rows.blockout, rows.lastcharge + STORAGE[].slot)
    rows.blockout <= b.limits.max_block_output_bytes ||
        throw(LimitError(:max_block_output_bytes, rows.blockout, b.limits.max_block_output_bytes, :max_block_output_bytes, :decode))
    if rows.remaining == 0
        d = rows.decoder
        d.pos == length(rows.bytes) + 1 || r.validate !== :strict || throw(DataError("block datums did not consume the block exactly", d.pos))
        release!(b, bytesbytes(length(rows.bytes)))
    end
    return (v, nothing)
end

function decoderow(rows::Rows)
    d = rows.decoder
    mode = getfield(rows, :mode)
    if mode === :typed
        v = decodetyped(rows.plan isa TypedPlan ? rows.T : Nothing, rows.plan, d, rows.adm)
        return finishtyped(rows.plan, v, rows.adm)                      # semantic conversion in caller space
    end
    if mode === :generic && rows.select !== nothing
        p = rows.plan
        vals = projectrow(p, d, rows.select, rows.outschema)
        return Row(Record(rows.outschema, vals, Val(:unchecked)), rows.adm)
    end
    v = decode(rows.plan, d)
    mode === :generic && return Row(v::Record, rows.adm)
    return v
end

"Decode one record keeping the selected fields (in the caller's order); the rest are skipped."
function projectrow(p::RecordPlan, d::Decoder, sel::Vector{Int}, out::RecordSchema)
    enter!(d)
    nf = length(p.fields)
    reserve!(d.budget, recordbytes(length(sel)) + vectorbytes(Any, nf))
    kept = Vector{Any}(undef, nf)
    allocated!(d.budget, vectorbytes(Any, nf))
    for (i, f) in enumerate(p.fields)
        if i in sel
            v = decode(f, d)
            b = p.boxes[i]
            b > 0 && reserve!(d.budget, b)
            kept[i] = v                                        # an isbits value boxes on assignment
            b > 0 && allocated!(d.budget, b)
        else
            skip(f, d)
        end
    end
    leave!(d)
    out = Any[kept[i] for i in sel]
    allocated!(d.budget, recordbytes(length(sel)))
    release!(d.budget, vectorbytes(Any, nf))                   # the projection scratch dies with this frame
    return out
end

function projectrow(p::ResolvedRecordPlan, d::Decoder, sel::Vector{Int}, out::RecordSchema)
    enter!(d)
    nf = length(p.schema.fields)
    reserve!(d.budget, recordbytes(length(sel)) + vectorbytes(Any, nf))
    kept = Vector{Any}(undef, nf)
    allocated!(d.budget, vectorbytes(Any, nf))
    for (slot, plan) in p.steps
        if slot != 0 && slot in sel
            v = decode(plan, d)
            b = p.boxes[slot]
            b > 0 && reserve!(d.budget, b)
            kept[slot] = v                                     # an isbits value boxes on assignment
            b > 0 && allocated!(d.budget, b)
        else
            skip(plan, d)
        end
    end
    for (slot, dp) in p.defaults
        slot in sel || continue
        countvalues!(d.budget)
        b = p.boxes[slot]
        b > 0 && reserve!(d.budget, b)
        kept[slot] = jsonvalue(dp.schema, dp.json, d.budget)
        b > 0 && allocated!(d.budget, b)
    end
    leave!(d)
    out = Any[kept[i] for i in sel]
    allocated!(d.budget, recordbytes(length(sel)))
    release!(d.budget, vectorbytes(Any, nf))                   # the projection scratch dies with this frame
    return out
end

"Per-block partitions of generic-mode rows: each block materialised as an `Avro.Table` when iterated."
struct RowsPartitions
    rows::Rows
end

function Tables.partitions(rows::Rows)
    getfield(rows, :mode) === :generic || throw(ArgumentError("only the generic record mode has Tables partitions"))
    return RowsPartitions(rows)
end

Base.IteratorSize(::Type{RowsPartitions}) = Base.SizeUnknown()
Base.eltype(::Type{RowsPartitions}) = Table

function Base.iterate(it::RowsPartitions, ::Nothing=nothing)
    rows = it.rows
    rows.remaining == 0 || throw(ArgumentError("Tables.partitions cannot start mid-block; iterate one interface only"))
    r = rows.reader
    blk = nextblock!(r; walk=false)
    blk === nothing && return nothing
    count, bytes = blk
    b = r.budget
    baseline = b.reserved
    try
        reserve!(b, bytesbytes(length(bytes)))
        allocated!(b, bytesbytes(length(bytes)))               # decompressed by nextblock!; already resident
        before = b.reserved
        d = Decoder(bytes, b; validate=r.validate)
        plan = rows.plan
        cols = columnbuilders(plan, rows.select, count, b)
        for _ in 1:count
            countvalues!(b)
            decoderow!(cols, d, plan)
        end
        d.pos == length(bytes) + 1 || throw(DataError("block datums did not consume the block exactly", d.pos))
        blockout = max(b.reserved - before, 0)
        blockout <= r.limits.max_block_output_bytes ||
            throw(LimitError(:max_block_output_bytes, blockout, r.limits.max_block_output_bytes, :max_block_output_bytes, :decode))
        release!(b, bytesbytes(length(bytes)))
        out = rows.outschema
        sel = rows.select === nothing ? collect(eachindex(out.fields)) : rows.select
        finals = AbstractVector[]
        for i in sel
            push!(finals, finishcolumn!(cols[i]::TypedColumn, b))
        end
        t = Table(out, r.schema, admitnames(out, rows.adm), finals, count, [1:count], r.metadata, r.codecname, r.sync)
        return (t, nothing)
    finally
        release!(b, max(b.reserved - baseline, 0))       # completed output transfers at return; partial output dies here
    end
end

"""
Column materialisation of generic-mode rows: Tables' generic row fallback cannot allocate columns from
a stored `Tables.Schema{nothing,nothing}`, so `Rows` materialises through its own per-block column
builders (also faster). The result is an `Avro.Table` over the not-yet-iterated remainder.
"""
function Tables.columns(rows::Rows)
    out = getfield(rows, :outschema)
    rows.remaining == 0 || throw(ArgumentError("Tables.columns cannot start mid-block; iterate one interface only"))
    b = rows.reader.budget
    baseline = b.reserved
    try
        colstypes = Type[juliatype(f.schema) for f in out.fields]
        finals, counts = decodestreamed!(rows.reader, rows.plan, rows.select, colstypes)
        nrows = sum(counts; init=0)
        ranges = UnitRange{Int}[]
        off = 0
        for count in counts
            push!(ranges, off + 1:off + count)
            off += count
        end
        t = Table(out, writerschema(rows), rowsymbols(rows), finals, nrows, ranges, metadata(rows), codec(rows), sync(rows))
        return t
    finally
        release!(b, max(b.reserved - baseline, 0))       # completed output transfers at return; partial output dies here
    end
end

retainedschema(x) = nothing
retainedschema(t::Table) = getfield(t, :schema)
retainedschema(rows::Rows) = schema(rows)
retainedschema(r::Reader) = r.schema
