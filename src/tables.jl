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

function rowwrapperbytes()
    return sizeof(Row) - STORAGE[].record
end

function Row(record::Record; names=DEFAULT_ADMISSION)
    return Row(record, admission(names))
end

function admitnames(s::RecordSchema, adm, budget::Union{Nothing,Budget}=nothing)
    budget === nothing &&
        return Symbol[admit!(adm, f.name) for f in s.fields]
    charge = vectorbytes(Symbol, length(s.fields))
    reserve!(budget, charge)
    names = Vector{Symbol}(undef, length(s.fields))
    allocated!(budget, charge)
    for (index, field) in enumerate(s.fields)
        names[index] = admit!(adm, field.name; budget=budget)
    end
    return names
end

function Tables.columnnames(r::Row)
    return admitnames(getfield(getfield(r, :record), :schema), getfield(r, :admission))
end

function Tables.getcolumn(r::Row, i::Int)
    return getfield(getfield(r, :record), :values)[i]
end

function Tables.getcolumn(r::Row, nm::Symbol)
    return getfield(r, :record)[nm]
end

function Base.show(io::IO, r::Row)
    return (print(io, "Avro.Row"); show(io, getfield(r, :record)))
end

"The wrapped `Avro.Record` of a row."
function record(r::Row)
    return getfield(r, :record)
end

# ---- projection -------------------------------------------------------------------------------------

"The `select=` field positions of `s` in the caller's order (unknown and duplicate names are errors)."
function selectindices(s::RecordSchema, select, budget::Budget)
    nf = length(s.fields)
    trait = Base.IteratorSize(typeof(select))
    capacity = trait isa Union{Base.HasLength,Base.HasShape} ? min(length(select), nf) : 0
    capacity >= 0 || throw(ArgumentError("select= length must be non-negative"))
    selected = BuildBuf{Int}(budget, capacity)
    slotcharge = vectorbytes(Int, nf)
    reserve!(budget, slotcharge)
    slots = zeros(Int, nf)
    allocated!(budget, slotcharge)
    for nm in select
        (nm isa Symbol || nm isa AbstractString) || throw(ArgumentError("select= takes column names (Symbols or strings), got $(typeof(nm))"))
        sizeof(nm) <= budget.limits.max_name_bytes ||
            throw(LimitError(:max_name_bytes, sizeof(nm), budget.limits.max_name_bytes,
                             :max_name_bytes, :decode))
        owned = nm isa Symbol
        name = owned ? ownedstringcopy(nm, budget) : nm
        try
            i = budgetedget(s.fieldindex, name, 0, budget)
            i == 0 && throw(ArgumentError("select: the record has no selected field with that name"))
            slots[i] == 0 || throw(ArgumentError("select: a field name appears more than once"))
            push!(selected, budget, i)
            slots[i] = selected.len
        finally
            owned && release!(budget, stringbytes(sizeof(name)))
        end
    end
    indices = finishbuild!(selected, budget)
    charge = sizeof(Projection)
    reserve!(budget, charge)
    projection = Projection(indices, slots)
    allocated!(budget, charge)
    return projection
end

"""
The derived effective schema of a projection: the selected fields in the selected order with names,
types, defaults, aliases, order, docs and props unchanged; a recursive root is substituted graph-wide.
"""
function projectschema(s::RecordSchema, sel::Vector{Int}, limits::Limits,
                       budget::Budget)
    return withconstructionbudget(budget) do _
        n = length(sel)
        charge = frozendictshell() + vectorbytes(Field, n) + vectorbytes(String, n) + vectorbytes(Int, n) + 128
        reserve!(budget, charge)
        memo = FrozenDict{String,Schema}()
        fields = emptywithcapacity(FrozenVector{Field}, n)
        index = emptywithcapacity(FrozenDict{String,Int}, n)
        name = copyfullname(s.name, budget)
        aliases = copystringvector(s.aliases, budget)
        rawaliases = copystringvector(s.rawaliases, budget)
        doc = s.doc === nothing ? nothing : ownedstringcopy(s.doc, budget)
        props = copyprops(s.props, budget, 2)
        meta = copiedmeta(s, sel, budget)
        out = publicnode(RecordSchema(name, aliases, rawaliases, doc, s.iserror,
                                      props, fields, index, meta))
        allocated!(budget, charge)
        try
            rootkey = ownedfullname(s.name, budget)
            budgetedinsert!(memo, rootkey, out, budget)
            for (k, i) in enumerate(sel)
                f = s.fields[i]
                field = copyfield(f, deepcopyschema(f.schema, memo), budget, 3)
                budgetedpush!(fields, field, budget)
                budgetedinsert!(index, field.name, k, budget)
            end
            freeze!(fields)
            freeze!(index)
            return finalizepublic!(out, limits, 0, 0)
        finally
            releaseownedmemo!(budget, memo)
        end
    end
end

# ---- the effective schema and plan of a container operation ------------------------------------------

function effectiveplan(r::Reader, reader_schema, union_resolution::Symbol, limits::Limits, decimal_byteorder::Symbol)
    reader_schema === nothing && return (r.schema, r.plan)
    plan = resolvingplan(r.schema, reader_schema; union_resolution=union_resolution, limits=limits,
                         budget=r.budget, decimal_little=decimal_byteorder === :little)
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
    tablesschema::Tables.Schema{nothing,nothing}
    columns::Vector{AbstractVector}
    nrows::Int
    blockranges::Vector{UnitRange{Int}}
    metadata::Map{Vector{UInt8}}
    codecname::Symbol
    syncmarker::NTuple{16,UInt8}
end

function exactvector(::Type{T}, n::Int, budget::Budget) where {T}
    charge = vectorbytes(T, n)
    reserve!(budget, charge)
    values = Vector{T}(undef, n)
    allocated!(budget, charge)
    return values
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
        projection = select === nothing ? nothing : selectindices(effective, select, r.budget)
        sel = projection === nothing ? nothing : projection.indices
        outschema = sel === nothing ? effective : projectschema(effective, sel, limits,
                                                                 r.budget)
        nkept = length(outschema.fields)
        keptowned = sel === nothing
        keptidx = keptowned ? exactvector(Int, nkept, r.budget) : sel
        keptowned && copyto!(keptidx, 1:nkept)
        colstypes = exactvector(Type, nkept, r.budget)
        for (index, field) in enumerate(outschema.fields)
            colstypes[index] = juliatype(field.schema)
        end
        if r.source isa BytesSource
            # byte and mapped sources: pre-scan, exact final preallocation, direct/parallel decode (§4.9)
            pre = prescanblocks(r)
            nrows = pre.totalrows
            finals = exactvector(AbstractVector, nkept, r.budget)
            for (index, E) in enumerate(colstypes)
                reserve!(r.budget, vectorbytes(E, nrows))
                finals[index] = Vector{E}(undef, nrows)
                allocated!(r.budget, vectorbytes(E, nrows))
            end
            decodeblocks!(r, plan, projection, finals, keptidx, colstypes, pre, taskcount,
                          reader_schema !== nothing)
            counts = exactvector(Int, length(pre.entries), r.budget)
            for (index, entry) in enumerate(pre.entries)
                counts[index] = entry.count
            end
            release!(r.budget, blocktablecharge(capacity(pre.entries)))
        else
            finals, counts = decodestreamed!(r, plan, projection, colstypes,
                                             reader_schema !== nothing)
        end
        nrows = sum(counts; init=0)
        ranges = exactvector(UnitRange{Int}, length(counts), r.budget)
        off = 0
        for (index, c) in enumerate(counts)
            ranges[index] = off + 1:off + c
            off += c
        end
        outnames = admitnames(outschema, adm, r.budget)
        tablesschema = storedschema(outnames, outschema, r.budget)
        reserve!(r.budget, shellbytes(Table))
        table = Table(outschema, r.schema, outnames, tablesschema, finals, nrows,
                      ranges, r.metadata, r.codecname, r.sync)
        allocated!(r.budget, shellbytes(Table))
        release!(r.budget, vectorbytes(Type, length(colstypes)))
        release!(r.budget, vectorbytes(Int, length(counts)))
        keptowned && release!(r.budget, vectorbytes(Int, length(keptidx)))
        projection === nothing || release!(r.budget, projectionbytes(projection))
        return table
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
function storedschema(names::Vector{Symbol}, s::RecordSchema)
    return Tables.Schema(names, Type[juliatype(f.schema) for f in s.fields]; stored=true)
end

function storedschema(names::Vector{Symbol}, s::RecordSchema, budget::Budget)
    n = length(s.fields)
    scratch = vectorbytes(Type, n)
    checkpoint = budgetcheckpoint(budget)
    types = nothing
    try
        reserve!(budget, scratch)
        types = Vector{Type}(undef, n)
        allocated!(budget, scratch)
        for (index, field) in enumerate(s.fields)
            types[index] = juliatype(field.schema)
        end
        retained = checked_add(checked_add(vectorbytes(Symbol, n), vectorbytes(Type, n)),
                               shellbytes(Tables.Schema{nothing,nothing}))
        reserve!(budget, retained)
        value = Tables.Schema(names, types; stored=true)
        allocated!(budget, retained)
        types = nothing
        release!(budget, scratch)
        return value
    catch
        types = nothing
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
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
    i = findfirst(==(nm), getfield(t, :names))
    i === nothing && throw(ArgumentError("no column $nm"))
    return getfield(t, :columns)[i]
end

function Tables.partitions(t::Table)
    return (subtable(t, r) for r in getfield(t, :blockranges))
end

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
                 getfield(t, :tablesschema),
                 AbstractVector[view(c, range) for c in getfield(t, :columns)], length(range), [1:length(range)],
                 getfield(t, :metadata), getfield(t, :codecname), getfield(t, :syncmarker))
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
    return (k for k in getfield(t, :metadata).keys)
end

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
function decodestreamed!(r::Reader, plan, projection::Union{Nothing,Projection},
                         colstypes::Vector{Type}, resolved::Bool=false)
    countbuilder = BuildBuf{Int}(r.budget, 0)
    chunkbuilder = BuildBuf{Vector{AbstractVector}}(r.budget, 0)
    slotrow = sum(slotbytes, colstypes; init=0)
    while (blk = nextblock!(r; walk=false)) !== nothing
        count, bytes, blockwork = blk
        reserve!(r.budget, bytesbytes(length(bytes)))
        allocated!(r.budget, bytesbytes(length(bytes)))                     # decompressed by nextblock!; already resident
        d = setblockscope!(Decoder(bytes, r.budget; validate=r.validate),
                           blockwork)
        cols = columnbuilders(plan, projection, count, r.budget)
        cells = plan isa RecordPlan ? fuseskips(cols, r.budget) : cols
        try
            outputbase = r.budget.reserved
            cap = r.limits.max_block_output_bytes
            decodeblockrows!(r.span, d, plan, cells, count, r.budget,
                             projection, resolved, outputbase, slotrow, cap)
            d.pos == length(bytes) + 1 || throw(DataError("block datums did not consume the block exactly", d.pos))
            finishblockwork!(r.budget, blockwork)
            release!(r.budget, bytesbytes(length(bytes)))
            keep = exactvector(AbstractVector, length(colstypes), r.budget)
            for (position, i) in enumerate(projection === nothing ?
                                           eachindex(cols) : projection.indices)
                keep[position] = finishcolumn!(cols[i]::TypedColumn, r.budget)
            end
            push!(chunkbuilder, r.budget, keep)
            push!(countbuilder, r.budget, count)
        finally
            releasefused!(cells, cols, r.budget)
            releasecolumnbuilders!(cols, r.budget)
        end
    end
    chunkcols = finishbuild!(chunkbuilder, r.budget)
    counts = finishbuild!(countbuilder, r.budget)
    nrows = sum(counts; init=0)
    finals = exactvector(AbstractVector, length(colstypes), r.budget)
    for (index, E) in enumerate(colstypes)             # both sets of reference slots coexist during assembly (plan §4.4)
        reserve!(r.budget, vectorbytes(E, nrows))
        finals[index] = Vector{E}(undef, nrows)
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
        release!(r.budget, vectorbytes(AbstractVector, length(chunk)))
    end
    release!(r.budget, vectorbytes(Vector{AbstractVector}, length(chunkcols)))
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
    const select::Union{Nothing,Projection}
    const resolved::Bool                     # the plan can add zero-input reader defaults
    const nrows::Int                       # exact datum count when L (0 otherwise)
    symbols::Union{Nothing,Vector{Symbol}} # lazy
    tablesschema::Union{Nothing,Tables.Schema{nothing,nothing}} # lazy
    bytes::Vector{UInt8}
    decoder::Union{Nothing,Decoder{Vector{UInt8}}}
    remaining::Int
    lastcharge::Int
    lastwrapper::Int
    blockout::Int
    blockwork::Union{Nothing,BlockWork}
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
        projection = select === nothing ? nothing : selectindices(effective, select, r.budget)
        sel = projection === nothing ? nothing : projection.indices
        outschema = mode === :generic ?
            (sel === nothing ? effective : projectschema(effective, sel, limits,
                                                          r.budget)) : nothing
        rowplan = mode === :typed ? typedplan(T, effective, plan, limits; budget=r.budget) : plan
        nrows = -1
        if r.source isa BytesSource
            pre = prescanblocks(r)                      # headers only; the table charge is transient
            release!(r.budget, blocktablecharge(max(64, nextpow2rows(length(pre.entries)))))
            pre.pending === nothing && (nrows = pre.totalrows)   # a structural error keeps SizeUnknown
        end                                                       # so acceptance matches streamed sources
        if nrows >= 0
            charge = shellbytes(Rows{true})
            reserve!(r.budget, charge)
            rows = Rows{true}(r, mode, effective, outschema, rowplan, T, adm,
                              projection, reader_schema !== nothing, nrows,
                              nothing, nothing, UInt8[], nothing,
                              0, 0, 0, 0, nothing)
            allocated!(r.budget, charge)
            return rows
        end
        charge = shellbytes(Rows{false})
        reserve!(r.budget, charge)
        rows = Rows{false}(r, mode, effective, outschema, rowplan, T, adm,
                           projection, reader_schema !== nothing, 0,
                           nothing, nothing, UInt8[], nothing,
                           0, 0, 0, 0, nothing)
        allocated!(r.budget, charge)
        return rows
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

function Base.close(rows::Rows)
    reader = getfield(rows, :reader)
    reader.closed && return nothing
    budget = reader.budget
    release!(budget, rows.lastcharge)
    rows.lastcharge = 0
    rows.lastwrapper = 0
    if rows.decoder !== nothing
        rows.blockwork = nothing
        releaseblock!(rows, budget)
    end
    return close(reader)
end

function schema(rows::Rows)
    return something(getfield(rows, :outschema), getfield(rows, :effective))
end

function writerschema(rows::Rows)
    return getfield(rows, :reader).schema
end

function metadata(rows::Rows)
    return getfield(rows, :reader).metadata
end

function codec(rows::Rows)
    return getfield(rows, :reader).codecname
end

function sync(rows::Rows)
    return getfield(rows, :reader).sync
end

function rowsymbols(rows::Rows)
    getfield(rows, :mode) === :generic || throw(ArgumentError("only the generic record mode has columns"))
    s = getfield(rows, :symbols)
    s === nothing || return s
    s = admitnames(getfield(rows, :outschema), getfield(rows, :adm), getfield(rows, :reader).budget)   # names admit lazily, on first request
    setfield!(rows, :symbols, s)
    return s
end

function Tables.istable(rows::Rows)
    return getfield(rows, :mode) === :generic
end

function Tables.rowaccess(rows::Rows)
    return getfield(rows, :mode) === :generic
end

function Tables.rows(rows::Rows)
    return rows
end

function Tables.schema(rows::Rows)
    cached = getfield(rows, :tablesschema)
    cached === nothing || return cached
    value = storedschema(rowsymbols(rows), getfield(rows, :outschema),
                         getfield(rows, :reader).budget)
    setfield!(rows, :tablesschema, value)
    return value
end

function Tables.columnnames(rows::Rows)
    return rowsymbols(rows)
end

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
    checkopen(r)
    b = r.budget
    release!(b, rows.lastcharge)
    rows.lastcharge = 0
    rows.lastwrapper = 0
    while rows.remaining == 0
        blk = nextblock!(r; walk=false)
        blk === nothing && return nothing
        rows.remaining = blk[1]
        rows.bytes = blk[2]
        rows.blockwork = blk[3]
        rows.blockout = 0
        reserve!(b, bytesbytes(length(rows.bytes)))
        allocated!(b, bytesbytes(length(rows.bytes)))          # decompressed by nextblock!; already resident
        rows.decoder = setblockscope!(Decoder(rows.bytes, b; validate=r.validate),
                                      rows.blockwork::BlockWork)
        if rows.remaining == 0
            validateemptyblock(rows.bytes, r.validate)
            finishblockwork!(b, rows.blockwork::BlockWork)
            rows.blockwork = nothing
            releaseblock!(rows, b)
        end
    end
    rows.remaining -= 1
    before = b.reserved
    decoder = rows.decoder::Decoder{Vector{UInt8}}
    datumspan!(r.span, decoder, decoder.pos, decoder.budget.values,
               decoder.budget.input_bytes)
    span = decoderspan(decoder)
    projection = getfield(rows, :mode) === :generic && rows.select !== nothing ?
        rows.select : nothing
    planneddatumspan!(rows.plan, decoder, span; resolved=rows.resolved,
                      projection=projection)
    span = decoderspan(decoder)
    datumwork = begindatum!(decoder, span)
    v = try
        decoderow(rows)
    catch
        abortdatum!(decoder, datumwork)
        rethrow()
    end
    finishdatum!(decoder, datumwork)
    rows.lastcharge = max(b.reserved - before, 0)
    rows.blockout = checked_add(rows.blockout,
                                rows.lastcharge - rows.lastwrapper +
                                representationslot(juliatype(rows.effective)))
    rows.blockout <= b.limits.max_block_output_bytes ||
        throw(LimitError(:max_block_output_bytes, rows.blockout, b.limits.max_block_output_bytes, :max_block_output_bytes, :decode))
    if rows.remaining == 0
        d = rows.decoder::Decoder{Vector{UInt8}}
        d.pos == length(rows.bytes) + 1 || r.validate !== :strict || throw(DataError("block datums did not consume the block exactly", d.pos))
        finishblockwork!(b, rows.blockwork::BlockWork)
        rows.blockwork = nothing
        releaseblock!(rows, b)
    end
    return (v, nothing)
end

function releaseblock!(rows::Rows, budget::Budget)
    charge = bytesbytes(length(rows.bytes))
    decoder = rows.decoder
    decoder === nothing || (decoder.buf = EMPTY_BYTES)
    rows.decoder = nothing
    rows.bytes = EMPTY_BYTES
    release!(budget, charge)
    return nothing
end

function decoderow(rows::Rows)
    d = rows.decoder
    mode = getfield(rows, :mode)
    if mode === :typed
        v = decodetyped(rows.plan isa TypedPlan ? rows.T : GenericDatumTarget, rows.plan, d, rows.adm)
        return finishtyped(rows.plan, v, rows.adm)                      # semantic conversion in caller space
    end
    if mode === :generic && rows.select !== nothing
        p = rows.plan
        vals = projectrow(p, d, rows.select, rows.outschema)
        wrapper = rowwrapperbytes()
        reserve!(d.budget, wrapper)
        row = Row(Record(rows.outschema, vals, Val(:unchecked)), rows.adm)
        allocated!(d.budget, recordbytes(length(vals)) - vectorbytes(Any, length(vals)) + wrapper)
        rows.lastwrapper = wrapper
        return row
    end
    v = decode(rows.plan, d)
    if mode === :generic
        wrapper = rowwrapperbytes()
        reserve!(d.budget, wrapper)
        row = Row(v::Record, rows.adm)
        allocated!(d.budget, wrapper)
        rows.lastwrapper = wrapper
        return row
    end
    return v
end

"Decode one record keeping the selected fields (in the caller's order); the rest are skipped."
function projectrow(p::RecordPlan, d::Decoder, projection::Projection, out::RecordSchema)
    enter!(d)
    n = length(projection.indices)
    reserve!(d.budget, recordbytes(n))
    kept = Vector{Any}(undef, n)
    allocated!(d.budget, vectorbytes(Any, n))
    for (i, f) in enumerate(p.fields)
        slot = projection.slots[i]
        if slot != 0
            v = decode(f, d)
            b = p.boxes[i]
            b > 0 && reserve!(d.budget, b)
            kept[slot] = v                                     # an isbits value boxes on assignment
            b > 0 && allocated!(d.budget, b)
        else
            skip(f, d)
        end
    end
    leave!(d)
    return kept
end

function projectrow(p::ResolvedRecordPlan, d::Decoder, projection::Projection, out::RecordSchema)
    enter!(d)
    n = length(projection.indices)
    reserve!(d.budget, recordbytes(n))
    kept = Vector{Any}(undef, n)
    allocated!(d.budget, vectorbytes(Any, n))
    for (slot, plan) in p.steps
        projected = slot == 0 ? 0 : projection.slots[slot]
        if projected != 0
            v = decode(plan, d)
            b = p.boxes[slot]
            b > 0 && reserve!(d.budget, b)
            kept[projected] = v                                # an isbits value boxes on assignment
            b > 0 && allocated!(d.budget, b)
        else
            skip(plan, d)
        end
    end
    for (slot, dp) in p.defaults
        projected = projection.slots[slot]
        projected == 0 && continue
        b = p.boxes[slot]
        b > 0 && reserve!(d.budget, b)
        kept[projected] = jsonvalue(dp, d.budget;
                                    depth=satadd(d.depth, 1))
        b > 0 && allocated!(d.budget, b)
    end
    leave!(d)
    return kept
end

"Per-block partitions of generic-mode rows: each block materialised as an `Avro.Table` when iterated."
struct RowsPartitions
    rows::Rows
end

function Tables.partitions(rows::Rows)
    getfield(rows, :mode) === :generic || throw(ArgumentError("only the generic record mode has Tables partitions"))
    return RowsPartitions(rows)
end

function Base.IteratorSize(::Type{RowsPartitions})
    return Base.SizeUnknown()
end

function Base.eltype(::Type{RowsPartitions})
    return Table
end

function Base.iterate(it::RowsPartitions, ::Nothing=nothing)
    rows = it.rows
    rows.remaining == 0 || throw(ArgumentError("Tables.partitions cannot start mid-block; iterate one interface only"))
    r = rows.reader
    blk = nextblock!(r; walk=false)
    blk === nothing && return nothing
    count, bytes, blockwork = blk
    b = r.budget
    baseline = b.reserved
    try
        reserve!(b, bytesbytes(length(bytes)))
        allocated!(b, bytesbytes(length(bytes)))               # decompressed by nextblock!; already resident
        before = b.reserved
        d = setblockscope!(Decoder(bytes, b; validate=r.validate), blockwork)
        plan = rows.plan
        cols = columnbuilders(plan, rows.select, count, b)
        for _ in 1:count
            datumspan!(r.span, d, d.pos, d.budget.values,
                       d.budget.input_bytes)
            span = decoderspan(d)
            planneddatumspan!(plan, d, span; resolved=rows.resolved,
                              projection=rows.select)
            span = decoderspan(d)
            datumwork = begindatum!(d, span)
            try
                countvalues!(b)
                decoderow!(cols, d, plan)
            catch
                abortdatum!(d, datumwork)
                rethrow()
            end
            finishdatum!(d, datumwork)
        end
        d.pos == length(bytes) + 1 || throw(DataError("block datums did not consume the block exactly", d.pos))
        finishblockwork!(b, blockwork)
        blockout = max(b.reserved - before, 0)
        blockout <= r.limits.max_block_output_bytes ||
            throw(LimitError(:max_block_output_bytes, blockout, r.limits.max_block_output_bytes, :max_block_output_bytes, :decode))
        release!(b, bytesbytes(length(bytes)))
        out = rows.outschema
        sel = rows.select === nothing ? eachindex(out.fields) : rows.select.indices
        finals = exactvector(AbstractVector, length(out.fields), b)
        for (position, i) in enumerate(sel)
            finals[position] = finishcolumn!(cols[i]::TypedColumn, b)
        end
        releasecolumnbuilders!(cols, b)
        outnames = admitnames(out, rows.adm, b)
        tableschema = storedschema(outnames, out, b)
        ranges = exactvector(UnitRange{Int}, 1, b)
        ranges[1] = 1:count
        reserve!(b, shellbytes(Table))
        t = Table(out, r.schema, outnames, tableschema, finals, count, ranges,
                  r.metadata, r.codecname, r.sync)
        allocated!(b, shellbytes(Table))
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
        colstypes = exactvector(Type, length(out.fields), b)
        for (index, field) in enumerate(out.fields)
            colstypes[index] = juliatype(field.schema)
        end
        finals, counts = decodestreamed!(rows.reader, rows.plan, rows.select,
                                         colstypes, rows.resolved)
        nrows = sum(counts; init=0)
        ranges = exactvector(UnitRange{Int}, length(counts), b)
        off = 0
        for (index, count) in enumerate(counts)
            ranges[index] = off + 1:off + count
            off += count
        end
        outnames = rowsymbols(rows)
        tableschema = storedschema(outnames, out, b)
        reserve!(b, shellbytes(Table))
        t = Table(out, writerschema(rows), outnames, tableschema, finals, nrows,
                  ranges, metadata(rows), codec(rows), sync(rows))
        allocated!(b, shellbytes(Table))
        release!(b, vectorbytes(Int, length(counts)))
        return t
    finally
        release!(b, max(b.reserved - baseline, 0))       # completed output transfers at return; partial output dies here
    end
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
