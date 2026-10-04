# Schema resolution (plan §4.7): `resolve(writer, reader)` builds a writer-driven read plan whose values
# are the reader schema's generic values (§4.6). Pairs are memoised on (writer, reader) node ids and
# every match attempt, memo step and plan node is charged to `max_resolution_work`; unions follow the
# spec policy (the first reader branch matching with promotion) or Apache Java's exact-match-first
# policy; failures are `ResolutionError`s carrying both paths.

"""
    Avro.ResolvedSchema

The result of `Avro.resolve`: the writer and reader schemas, the union policy and the resolving read
plan (`plan`), which `DatumReader(writer; reader_schema=reader)` uses.
"""
struct ResolvedSchema
    writer::Schema
    reader::Schema
    union_resolution::Symbol
    plan::ReadPlan
end

function Base.show(io::IO, r::ResolvedSchema)
    return print(io, "Avro.ResolvedSchema(", kind(r.writer), " → ", kind(r.reader), ", ", r.union_resolution, ")")
end

# ---- resolving plan nodes ---------------------------------------------------------------------------

"A promotable writer leaf decoded as its raw primitive and interpreted by the reader's leaf plan."
struct PromotePlan <: ReadPlan
    writer::Symbol        # :int, :long, :float, :double, :bytes or :string
    reader::ReadPlan
end

"A reader-only field: its default materialised afresh for every record."
struct DefaultPlan <: ReadPlan
    schema::Schema
    json::Any
    branch::Int
    branches::Vector{Int}
    values::Int
    maxdepth::Int
end

const EMPTY_DEFAULT_BRANCHES = Int[]

"Writer enum positions remapped to the reader's (0 = absent: the reader default, else an error)."
struct EnumRemapPlan <: ReadPlan
    writer::EnumSchema
    reader::EnumSchema
    map::Vector{Int32}
    default::Int32
    writerpath::String
    readerpath::String
end

"A writer union branch with no resolving reader branch: raises when a datum selects it; skips as the writer."
struct UnresolvableBranch <: ReadPlan
    msg::String
    writerpath::String
    readerpath::String
    skipper::ReadPlan
end

"A writer union resolved branch by branch; the output follows the reader (bare, nullable or UnionValue)."
struct UnionResolvePlan <: ReadPlan
    branches::Vector{ReadPlan}
    readerindex::Vector{Int}    # the reader branch per writer branch (0 for a non-union reader)
    nullable::Int               # the reader's null position in the two-branch nullable form, 0 otherwise
    readerunion::Bool
end

"A non-union writer resolved against the selected reader union branch."
struct WrapPlan <: ReadPlan
    inner::ReadPlan
    readerindex::Int
    nullable::Int
end

"A record resolved field by field: writer-ordered steps into reader slots (0 = skip), then the defaults."
mutable struct ResolvedRecordPlan <: ReadPlan
    const schema::RecordSchema                   # the reader record
    const steps::Vector{Pair{Int,ReadPlan}}
    const defaults::Vector{Pair{Int,DefaultPlan}}
    const boxes::Vector{Int}
end

function isresolving(::ReadPlan)
    return false
end

function isresolving(::Union{PromotePlan,DefaultPlan,EnumRemapPlan,UnresolvableBranch,UnionResolvePlan,WrapPlan,ResolvedRecordPlan})
    return true
end

"Compile the exact value count and every selected union branch of one reader default."
function defaultwork(schema::Schema, json, branch::Int, budget::Budget)
    checkpoint = budgetcheckpoint(budget)
    branches = BuildBuf{Int}(budget, 0)
    maxdepth = Ref(0)
    try
        values = defaultvaluecount(schema, json, budget, 1, branches, maxdepth,
                                   branch)
        tape = finishbuild!(branches, budget)
        if isempty(tape)
            release!(budget, vectorbytes(Int, 0))
            tape = EMPTY_DEFAULT_BRANCHES
        end
        return (values=values, branches=tape, maxdepth=maxdepth[])
    catch
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

function defaultbranchbytes(branches::Vector{Int})
    return branches === EMPTY_DEFAULT_BRANCHES ? 0 : vectorbytes(Int, length(branches))
end

function defaultvaluecount(schema::Schema, json, budget::Budget, depth::Int,
                           branches::BuildBuf{Int}, maxdepth::Base.RefValue{Int},
                           selected::Int=0)
    selected == 0 || throw(ArgumentError("default branch given for a non-union schema"))
    addresolution!(budget)
    checkdepth(budget, depth)
    return 1
end

function defaultvaluecount(schema::ArraySchema, json::JSONArray, budget::Budget,
                           depth::Int, branches::BuildBuf{Int},
                           maxdepth::Base.RefValue{Int}, selected::Int=0)
    selected == 0 || throw(ArgumentError("default branch given for a non-union schema"))
    addresolution!(budget)
    checkdepth(budget, depth)
    maxdepth[] = max(maxdepth[], depth)
    values = 1
    for item in json
        values = checked_add(values,
                             defaultvaluecount(schema.items, item, budget, depth + 1,
                                               branches, maxdepth))
    end
    return values
end

function defaultvaluecount(schema::MapSchema, json::JSONObject, budget::Budget,
                           depth::Int, branches::BuildBuf{Int},
                           maxdepth::Base.RefValue{Int}, selected::Int=0)
    selected == 0 || throw(ArgumentError("default branch given for a non-union schema"))
    addresolution!(budget)
    checkdepth(budget, depth)
    maxdepth[] = max(maxdepth[], depth)
    values = 1
    for key in json.order
        values = checked_add(values,
                             defaultvaluecount(schema.values,
                                               budgetedgetindex(json.members, key, budget),
                                               budget, depth + 1, branches,
                                               maxdepth))
    end
    return values
end

function defaultvaluecount(schema::RecordSchema, json::JSONObject, budget::Budget,
                           depth::Int, branches::BuildBuf{Int},
                           maxdepth::Base.RefValue{Int}, selected::Int=0)
    selected == 0 || throw(ArgumentError("default branch given for a non-union schema"))
    addresolution!(budget)
    checkdepth(budget, depth)
    maxdepth[] = max(maxdepth[], depth)
    values = 1
    for field in schema.fields
        child, childbranch = if budgetedhaskey(json.members, field.name, budget)
            (budgetedgetindex(json.members, field.name, budget), 0)
        else
            default = field.default
            default isa DefaultValue && default.valid ||
                throw(ArgumentError("validated record default is missing field $(field.name)"))
            (default.json, default.branch)
        end
        values = checked_add(values,
                             defaultvaluecount(field.schema, child, budget, depth + 1,
                                               branches, maxdepth, childbranch))
    end
    return values
end

function defaultvaluecount(schema::UnionSchema, json, budget::Budget, depth::Int,
                           branches::BuildBuf{Int},
                           maxdepth::Base.RefValue{Int}, selected::Int=0)
    addresolution!(budget)
    checkdepth(budget, depth)
    if selected == 0
        valid, selected = validatedefault(schema, json, budget.limits.max_depth,
                                          depth, budget)
        valid || throw(ArgumentError("validated union default matches no branch"))
    else
        1 <= selected <= length(schema.branches) ||
            throw(ArgumentError("invalid default union branch $selected"))
    end
    push!(branches, budget, selected)
    return checked_add(1,
                       defaultvaluecount(schema.branches[selected], json, budget,
                                         depth, branches, maxdepth))
end

# ---- the resolver -----------------------------------------------------------------------------------

mutable struct ResolveContext
    const budget::Budget
    const policy::Symbol
    const decimal_little::Bool
    const memokeys::Vector{Vector{Int32}}          # per writer node id: sorted reader node ids
    const memoplans::Vector{Vector{ReadPlan}}
    const memocaps::Vector{Int32}                  # prebuilt capacity per slot (§4.4 replacement growth)
    const readermemo::Vector{Union{Nothing,ReadPlan}}
    const writermemo::Vector{Union{Nothing,ReadPlan}}
end

const PATH_FIELD = UInt8(1)
const PATH_INDEX = UInt8(2)
const PATH_ITEMS = UInt8(3)
const PATH_VALUES = UInt8(4)

"One allocation-free segment in a charged mutable resolution path."
struct ResolutionPathSegment
    kind::UInt8
    name::Union{Nothing,String}
    index::Int
end

const ResolutionPath = BuildBuf{ResolutionPathSegment}

struct SchemaDescription
    schema::Schema
end

function pushfield!(path::ResolutionPath, name::String, budget::Budget)
    push!(path, budget, ResolutionPathSegment(PATH_FIELD, name, 0))
    return path
end

function pushindex!(path::ResolutionPath, index::Int, budget::Budget)
    push!(path, budget, ResolutionPathSegment(PATH_INDEX, nothing, index))
    return path
end

function pushitems!(path::ResolutionPath, budget::Budget)
    push!(path, budget, ResolutionPathSegment(PATH_ITEMS, nothing, 0))
    return path
end

function pushvalues!(path::ResolutionPath, budget::Budget)
    push!(path, budget, ResolutionPathSegment(PATH_VALUES, nothing, 0))
    return path
end

function poppath!(path::ResolutionPath)
    path.len > 0 || throw(ArgumentError("cannot pop an empty resolution path"))
    path.len -= 1
    return path
end

function pathstorage(path::ResolutionPath)
    return vectorbytes(ResolutionPathSegment, length(path.data)) + shellbytes(ResolutionPath)
end

function integerdigits(x::Int)
    u = x < 0 ? UInt(-(x + 1)) + UInt(1) : UInt(x)
    digits = 1
    while u >= UInt(10)
        u ÷= UInt(10)
        digits += 1
    end
    return digits + (x < 0)
end

function diagnosticpartbytes(x::AbstractString)
    return sizeof(x)
end

function diagnosticpartbytes(x::Symbol)
    return sizeof(x)
end

function diagnosticpartbytes(x::Integer)
    return integerdigits(Int(x))
end

function diagnosticpartbytes(x::FullName)
    return fullnamesize(x)
end

function diagnosticpartbytes(x::SchemaDescription)
    s = x.schema
    return sizeof(kind(s)) + (s isa NamedSchema ? 1 + fullnamesize(s.name) : 0)
end

function writediagnosticpart!(buf::Vector{UInt8}, pos::Int, x::AbstractString)
    n = sizeof(x)
    copyto!(buf, pos, codeunits(x), 1, n)
    return pos + n
end

function writediagnosticpart!(buf::Vector{UInt8}, pos::Int, x::Symbol)
    n = sizeof(x)
    ptr = Base.unsafe_convert(Ptr{UInt8}, x)
    @inbounds for i in 1:n
        buf[pos + i - 1] = unsafe_load(ptr, i)
    end
    return pos + n
end

function writediagnosticpart!(buf::Vector{UInt8}, pos::Int, x::Integer)
    value = Int(x)
    negative = value < 0
    u = negative ? UInt(-(value + 1)) + UInt(1) : UInt(value)
    digits = integerdigits(value) - negative
    negative && (buf[pos] = UInt8('-'); pos += 1)
    last = pos + digits - 1
    i = last
    while true
        u, digit = divrem(u, UInt(10))
        buf[i] = UInt8('0') + UInt8(digit)
        i == pos && break
        i -= 1
    end
    return last + 1
end

function writediagnosticpart!(buf::Vector{UInt8}, pos::Int, x::FullName)
    if !isempty(x.namespace)
        pos = writediagnosticpart!(buf, pos, x.namespace)
        buf[pos] = UInt8('.')
        pos += 1
    end
    return writediagnosticpart!(buf, pos, x.name)
end

function writediagnosticpart!(buf::Vector{UInt8}, pos::Int, x::SchemaDescription)
    s = x.schema
    pos = writediagnosticpart!(buf, pos, kind(s))
    if s isa NamedSchema
        buf[pos] = UInt8(' ')
        pos = writediagnosticpart!(buf, pos + 1, s.name)
    end
    return pos
end

"A quoted, escaped diagnostic prefix that never retains an attacker-sized string."
struct BoundedQuotedString{S<:Union{AbstractString,Symbol}}
    value::S
    stop::Int
end

function boundedquoted(value::S) where {S<:Union{AbstractString,Symbol}}
    return BoundedQuotedString{S}(value, encodekeyprefix(value))
end

function diagnosticpartbytes(x::BoundedQuotedString)
    suffix = x.stop < sizeof(x.value) ?
        checked_add(10, integerdigits(sizeof(x.value) - x.stop)) : 0
    return checked_add(checked_add(2, escapedkeybytes(x.value, x.stop)), suffix)
end

function writediagnosticpart!(buf::Vector{UInt8}, pos::Int,
                              x::BoundedQuotedString)
    buf[pos] = UInt8('"')
    pos = writeescapedkey!(buf, pos + 1, x.value, x.stop)
    buf[pos] = UInt8('"')
    pos += 1
    if x.stop < sizeof(x.value)
        pos = writediagnosticpart!(buf, pos, " (+")
        pos = writediagnosticpart!(buf, pos, sizeof(x.value) - x.stop)
        pos = writediagnosticpart!(buf, pos, " bytes)")
    end
    return pos
end

"Reserve the Vector-to-String ownership-transfer peak for one exact diagnostic."
function diagnosticbuffer(n::Int, budget::Budget)
    reserve!(budget, bytesbytes(n))
    buf = Vector{UInt8}(undef, n)
    allocated!(budget, bytesbytes(n))
    return buf
end

function finishdiagnostic!(buf::Vector{UInt8}, budget::Budget)
    return takeownedstring!(buf, budget)
end

"Build one exact, charged diagnostic string without intermediate concatenations."
function diagnosticstring(budget::Budget, parts...)
    n = 0
    for part in parts
        n = checked_add(n, diagnosticpartbytes(part))
    end
    checkpoint = budgetcheckpoint(budget)
    buf = nothing
    out = nothing
    try
        buf = diagnosticbuffer(n, budget)
        pos = 1
        for part in parts
            pos = writediagnosticpart!(buf, pos, part)
        end
        pos == n + 1 || throw(ArgumentError("diagnostic size mismatch"))
        out = finishdiagnostic!(buf, budget)
        buf = nothing
        return out
    catch
        buf = out = nothing
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

function diagnosticstring(::Nothing, parts...)
    n = 0
    for part in parts
        n = checked_add(n, diagnosticpartbytes(part))
    end
    buf = Vector{UInt8}(undef, n)
    pos = 1
    for part in parts
        pos = writediagnosticpart!(buf, pos, part)
    end
    pos == n + 1 || throw(ArgumentError("diagnostic size mismatch"))
    return String(buf)
end

function pathbytes(path::ResolutionPath)
    n = 1
    for i in 1:path.len
        segment = path.data[i]
        if segment.kind == PATH_FIELD
            n = checked_add(n, 1 + sizeof(segment.name::String))
        elseif segment.kind == PATH_INDEX
            n = checked_add(n, 2 + integerdigits(segment.index))
        elseif segment.kind == PATH_ITEMS
            n = checked_add(n, 7)
        else
            n = checked_add(n, 8)
        end
    end
    return n
end

"Format one charged path only when a diagnostic or retained plan node needs it."
function formatpath(path::ResolutionPath, budget::Budget)
    n = pathbytes(path)
    checkpoint = budgetcheckpoint(budget)
    buf = nothing
    out = nothing
    try
        buf = diagnosticbuffer(n, budget)
        pos = 1
        buf[pos] = UInt8('$')
        pos += 1
        for i in 1:path.len
            segment = path.data[i]
            if segment.kind == PATH_FIELD
                buf[pos] = UInt8('.')
                pos = writediagnosticpart!(buf, pos + 1, segment.name::String)
            elseif segment.kind == PATH_INDEX
                buf[pos] = UInt8('[')
                pos = writediagnosticpart!(buf, pos + 1, segment.index)
                buf[pos] = UInt8(']')
                pos += 1
            elseif segment.kind == PATH_ITEMS
                pos = writediagnosticpart!(buf, pos, "[items]")
            else
                pos = writediagnosticpart!(buf, pos, "[values]")
            end
        end
        pos == n + 1 || throw(ArgumentError("resolution path size mismatch"))
        out = finishdiagnostic!(buf, budget)
        buf = nothing
        return out
    catch
        buf = out = nothing
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

"The bytes of the resolution memo tables at their current capacities (charged storage)."
function memotablebytes(ctx::ResolveContext)
    total = 2 * vectorbytes(Vector{Int32}, length(ctx.memokeys)) + vectorbytes(Int32, length(ctx.memocaps))
    for c in ctx.memocaps
        total += vectorbytes(Int32, Int(c)) + vectorbytes(ReadPlan, Int(c))
    end
    return total
end

"""
    Avro.resolve(writer::Schema, reader::Schema; union_resolution=:spec, limits=Limits()) -> ResolvedSchema

Resolve data written with `writer` for a `reader` (plan §4.7): kinds, promotions
(`int→long/float/double`, `long→float/double`, `float→double`, `string↔bytes`), named types by fullname,
reader aliases or unqualified name, record fields by name and reader field aliases (an alias consumes the
writer field), reader-only fields from valid defaults, enum symbols by name with the reader default,
unions under `union_resolution=:spec` (first reader branch matching with promotion) or `:java`
(exact match first). Values follow the reader schema. Bounded by `max_resolution_work`.
"""
function resolve(writer::Schema, reader::Schema; union_resolution::Symbol=:spec, limits::Limits=Limits(),
                 budget::Union{Nothing,Budget}=nothing, decimal_little::Bool=false)
    union_resolution in (:spec, :java) || throw(ArgumentError("union_resolution must be :spec or :java"))
    budget === nothing &&
        return withbudget(b -> resolve(writer, reader; union_resolution=union_resolution, limits=limits,
                                       budget=b, decimal_little=decimal_little), limits)
    checkpoint = budgetcheckpoint(budget)
    try
        wnodes = graphinfo(writer).nodes
        rnodes = graphinfo(reader).nodes
        tables = 2 * vectorbytes(Vector{Int32}, wnodes) + vectorbytes(Int32, wnodes) + 2 * wnodes * STORAGE[].vector +
                 vectorbytes(Union{Nothing,ReadPlan}, rnodes) + vectorbytes(Union{Nothing,ReadPlan}, wnodes)
        reserve!(budget, tables)                           # the memo tables, released once the plan is built (§4.4)
        memokeys = Vector{Vector{Int32}}(undef, wnodes)
        memoplans = Vector{Vector{ReadPlan}}(undef, wnodes)
        for i in 1:wnodes
            memokeys[i] = Int32[]
            memoplans[i] = ReadPlan[]
        end
        ctx = ResolveContext(budget, union_resolution, decimal_little, memokeys, memoplans, zeros(Int32, wnodes),
                             Vector{Union{Nothing,ReadPlan}}(nothing, rnodes),
                             Vector{Union{Nothing,ReadPlan}}(nothing, wnodes))
        allocated!(budget, tables)
        writerpath = ResolutionPath(budget, 8)
        readerpath = ResolutionPath(budget, 8)
        plan = resolvenode(ctx, writer, reader, writerpath, readerpath)
        release!(budget, pathstorage(writerpath) + pathstorage(readerpath))
        release!(budget, memotablebytes(ctx) + vectorbytes(Union{Nothing,ReadPlan}, rnodes) +
                         vectorbytes(Union{Nothing,ReadPlan}, wnodes))     # construction memos die here
        return ResolvedSchema(writer, reader, union_resolution, plan)
    catch
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

"""
    resolvingplan(writer, reader; union_resolution=:spec, limits=Limits()) -> ReadPlan

The read plan decoding data written with `writer` into values of `reader`.
"""
function resolvingplan(writer::Schema, reader::Schema; union_resolution::Symbol=:spec, limits::Limits=Limits(),
                       budget::Union{Nothing,Budget}=nothing, decimal_little::Bool=false)
    return resolve(writer, reader; union_resolution=union_resolution, limits=limits, budget=budget,
                   decimal_little=decimal_little).plan
end

function reserror(ctx::ResolveContext, wp::ResolutionPath, rp::ResolutionPath, parts...)
    msg = diagnosticstring(ctx.budget, parts...)
    writerpath = formatpath(wp, ctx.budget)
    readerpath = formatpath(rp, ctx.budget)
    throw(ResolutionError(msg, writerpath, readerpath))
end

function readerplan(ctx::ResolveContext, s::Schema)
    return readplan(s, ctx.readermemo, ctx.budget, ctx.decimal_little)
end

function writerplan(ctx::ResolveContext, s::Schema)
    return readplan(s, ctx.writermemo, ctx.budget, ctx.decimal_little)
end

function memoslot(ctx::ResolveContext, w::Schema)
    return Int(nodeid(w)) + 1                          # the tables are prebuilt over every writer node
end

function memokey(r::Schema, javasoft::Bool)
    id = nodeid(r)
    return javasoft ? -id - Int32(1) : id
end

function memolookup(ctx::ResolveContext, w::Schema, r::Schema, javasoft::Bool=false)
    iw = memoslot(ctx, w)
    keys = ctx.memokeys[iw]
    ir = memokey(r, javasoft)
    i = resolutionsearchsortedfirst(keys, ir, ctx.budget)
    (i <= length(keys) && keys[i] == ir) && return ctx.memoplans[iw][i]
    return nothing
end

function memostore!(ctx::ResolveContext, w::Schema, r::Schema, p::ReadPlan,
                    javasoft::Bool=false)
    iw = memoslot(ctx, w)
    keys = ctx.memokeys[iw]
    ir = memokey(r, javasoft)
    i = resolutionsearchsortedfirst(keys, ir, ctx.budget)
    if i <= length(keys) && keys[i] == ir
        addresolution!(ctx.budget)                    # the replaced plan entry
        ctx.memoplans[iw][i] = p
        return p
    end
    if length(keys) == ctx.memocaps[iw]
        addresolution!(ctx.budget, checked_mul(2, length(keys))) # key and plan replacement copies
        newcap = max(2 * Int(ctx.memocaps[iw]), 4)
        growth = vectorbytes(Int32, newcap) + vectorbytes(ReadPlan, newcap)
        reserve!(ctx.budget, growth)                   # the replacement partner vectors (§4.4)
        nk = Vector{Int32}(undef, newcap)
        np = Vector{ReadPlan}(undef, newcap)
        allocated!(ctx.budget, growth)
        resize!(nk, length(keys))
        resize!(np, length(keys))
        copyto!(nk, 1, keys, 1, length(keys))
        copyto!(np, 1, ctx.memoplans[iw], 1, length(keys))
        old = vectorbytes(Int32, Int(ctx.memocaps[iw])) + vectorbytes(ReadPlan, Int(ctx.memocaps[iw]))
        ctx.memokeys[iw] = nk                          # the old vectors are unreachable only after the rebind
        ctx.memoplans[iw] = np
        ctx.memocaps[iw] = Int32(newcap)
        release!(ctx.budget, old)
        keys = nk
    end
    addresolution!(ctx.budget, checked_mul(2, length(keys) - i + 2)) # shifts plus new key and plan
    insert!(keys, i, ir)
    insert!(ctx.memoplans[iw], i, p)
    return p
end

# A failed subtree (a writer union branch that does not resolve) leaves no half-built entries behind.
function memoclear!(ctx::ResolveContext)
    foreach(empty!, ctx.memokeys)
    foreach(empty!, ctx.memoplans)
    return nothing
end

function resolvenode(ctx::ResolveContext, w::Schema, r::Schema,
                     wp::ResolutionPath, rp::ResolutionPath,
                     javasoft::Bool=false)
    addresolution!(ctx.budget, 1)
    cached = memolookup(ctx, w, r, javasoft)
    cached === nothing || return cached
    budgetedschemaequal(w, r, ctx.budget) &&
        return memostore!(ctx, w, r, readerplan(ctx, r), javasoft)
    return memostore!(ctx, w, r, resolvekinds(ctx, w, r, wp, rp, javasoft),
                      javasoft)
end

"Resolve the Java reader-union record candidate selected by structural soft matching."
function resolvesoftrecord(ctx::ResolveContext, writer::RecordSchema,
                           reader::RecordSchema,
                           writerpath::ResolutionPath,
                           readerpath::ResolutionPath)
    return resolvenode(ctx, writer, reader, writerpath, readerpath, true)
end

function describe(s::NamedSchema)
    return string(kind(s), " ", fullname(s))
end

function describe(s::Schema)
    return string(kind(s))
end

"Named types match by normalised fullname, by a reader alias, or by the unqualified name (the spec's record rule)."
function namesmatch(w::NamedSchema, r::NamedSchema, budget::Union{Nothing,Budget}=nothing)
    budget === nothing || addcompare!(budget, fullnamecomparisonwork(w.name, r.name))
    fullnameequal(w.name, r.name) && return true
    for alias in r.aliases
        budget === nothing || addcompare!(budget, fullnamecomparisonwork(w.name, alias))
        fullnameequal(w.name, alias) && return true
    end
    budget === nothing || addcompare!(budget, keycomparisonwork(w.name.name, r.name.name))
    return w.name.name == r.name.name
end

function resolvekinds(ctx::ResolveContext, w::Schema, r::Schema,
                      wp::ResolutionPath, rp::ResolutionPath,
                      javasoft::Bool=false)
    w isa UnionSchema && return resolvewriterunion(ctx, w, r, wp, rp, javasoft)
    r isa UnionSchema && return resolvereaderunion(ctx, w, r, wp, rp, javasoft)
    if r isa RecordSchema
        w isa RecordSchema || reserror(ctx, wp, rp, "writer ", SchemaDescription(w),
            " does not resolve to reader ", SchemaDescription(r))
        javasoft || namesmatch(w, r, ctx.budget) ||
            reserror(ctx, wp, rp, "writer ", SchemaDescription(w),
                     " does not match reader ", SchemaDescription(r), " by name or alias")
        return resolverecord(ctx, w, r, wp, rp, javasoft)
    end
    if r isa EnumSchema
        w isa EnumSchema || reserror(ctx, wp, rp, "writer ", SchemaDescription(w),
            " does not resolve to reader ", SchemaDescription(r))
        namesmatch(w, r, ctx.budget) || reserror(ctx, wp, rp, "writer ", SchemaDescription(w),
            " does not match reader ", SchemaDescription(r), " by name or alias")
        return resolveenum(ctx, w, r, wp, rp)
    end
    if r isa FixedSchema
        w isa FixedSchema || reserror(ctx, wp, rp, "writer ", SchemaDescription(w),
            " does not resolve to reader ", SchemaDescription(r))
        namesmatch(w, r, ctx.budget) || reserror(ctx, wp, rp, "writer ", SchemaDescription(w),
            " does not match reader ", SchemaDescription(r), " by name or alias")
        w.size == r.size || reserror(ctx, wp, rp, "fixed sizes differ (", w.size,
            " vs ", r.size, ")")
        checkdecimal(ctx, w, r, wp, rp)
        return readerplan(ctx, r)
    end
    if r isa ArraySchema
        w isa ArraySchema || reserror(ctx, wp, rp, "writer ", SchemaDescription(w),
            " does not resolve to reader array")
        pushitems!(wp, ctx.budget)
        pushitems!(rp, ctx.budget)
        items = try
            resolvenode(ctx, w.items, r.items, wp, rp, javasoft)
        finally
            poppath!(rp)
            poppath!(wp)
        end
        ms = minsize(w.items, ctx.budget)
        return plannode(() -> ArrayPlan(items, elementtype(r.items), ms), ctx.budget)
    end
    if r isa MapSchema
        w isa MapSchema || reserror(ctx, wp, rp, "writer ", SchemaDescription(w),
            " does not resolve to reader map")
        pushvalues!(wp, ctx.budget)
        pushvalues!(rp, ctx.budget)
        values = try
            resolvenode(ctx, w.values, r.values, wp, rp, javasoft)
        finally
            poppath!(rp)
            poppath!(wp)
        end
        ms = minsize(w.values, ctx.budget)
        return plannode(() -> MapPlan(values, elementtype(r.values), ms), ctx.budget)
    end
    return resolveleaf(ctx, w, r, wp, rp)
end

const PROMOTIONS = Dict{Symbol,Tuple{Vararg{Symbol}}}(:int => (:long, :float, :double), :long => (:float, :double), :float => (:double,),
                                                      :string => (:bytes,), :bytes => (:string,))

function checkdecimal(ctx::ResolveContext, w::Schema, r::Schema,
                      wp::ResolutionPath, rp::ResolutionPath)
    lw, lr = logical(w), logical(r)
    (lw isa DecimalLogical && lr isa DecimalLogical && (lw.precision != lr.precision || lw.scale != lr.scale)) &&
        reserror(ctx, wp, rp, "decimal precision/scale differ (", lw.precision, "/", lw.scale,
            " vs ", lr.precision, "/", lr.scale, ")")
    return nothing
end

function resolveleaf(ctx::ResolveContext, w::Schema, r::Schema,
                     wp::ResolutionPath, rp::ResolutionPath)
    kw, kr = kind(w), kind(r)
    checkdecimal(ctx, w, r, wp, rp)
    kw == kr && return readerplan(ctx, r)                        # same encoding; the reader's interpretation wins
    kr in get(PROMOTIONS, kw, ()) || reserror(ctx, wp, rp, "writer ", SchemaDescription(w),
        " does not resolve to reader ", SchemaDescription(r))
    reader = readerplan(ctx, r)
    return plannode(() -> PromotePlan(kw, reader), ctx.budget)
end

"Whether writer `w` matches reader branch `c` at the top level (the spec's matching rules, with promotion)."
function matches(w::Schema, c::Schema, budget::Union{Nothing,Budget}=nothing)
    c isa UnionSchema && return false
    w isa NamedSchema && return typeof(w) === typeof(c) && namesmatch(w, c, budget) &&
        (!(w isa FixedSchema) || w.size == c.size) && matchingdecimals(w, c)
    c isa NamedSchema && return false
    kw, kc = kind(w), kind(c)
    kw == kc && return matchingdecimals(w, c)
    return kc in get(PROMOTIONS, kw, ())
end

"Recognized decimal schemas match only when both precision and scale match."
function matchingdecimals(w::Schema, r::Schema)
    lw = logical(w)
    lr = logical(r)
    lw isa DecimalLogical && lr isa DecimalLogical || return true
    return lw.precision == lr.precision && lw.scale == lr.scale
end

"Apache Java's first pass: the same kind (named types by fullname or reader alias), no promotion."
function exactmatch(w::Schema, c::Schema, budget::Union{Nothing,Budget}=nothing)
    typeof(w) === typeof(c) || return false
    if w isa NamedSchema
        budgetedfullnameequal(w.name, c.name, budget) && return true
        for alias in c.aliases
            budgetedfullnameequal(w.name, alias, budget) && return true
        end
        return false
    end
    return true
end

struct BranchMatch
    index::Int
    softrecord::Bool
end

function softcandidate(ctx::ResolveContext, writer::RecordSchema, reader::Field)
    candidate = 0
    viaalias = false
    for name in Iterators.flatten(((reader.name,), reader.aliases))
        addresolution!(ctx.budget)
        index = budgetedget(writer.fieldindex, name, 0, ctx.budget)
        (index == 0 || index == candidate) && continue
        candidate == 0 || return (-1, false)
        candidate = index
        viaalias = name != reader.name
    end
    return (candidate, viaalias)
end

function softclaim(ctx::ResolveContext, writer::RecordSchema, reader::RecordSchema,
                   writerindex::Int)
    aliasclaim = 0
    nameclaim = 0
    for (readerindex, field) in enumerate(reader.fields)
        candidate, viaalias = softcandidate(ctx, writer, field)
        candidate >= 0 || return -1
        candidate == writerindex || continue
        if viaalias
            aliasclaim == 0 || return -1
            aliasclaim = readerindex
        else
            nameclaim == 0 || return -1
            nameclaim = readerindex
        end
    end
    return aliasclaim == 0 ? nameclaim : aliasclaim
end

function softpairpresent(path::BuildBuf{NTuple{2,Int32}}, pair::NTuple{2,Int32},
                         budget::Budget)
    for index in 1:path.len
        addresolution!(budget)
        @inbounds path.data[index] == pair && return true
    end
    return false
end

function softrootactionok(ctx::ResolveContext, writer::Schema, reader::Schema,
                          path::BuildBuf{NTuple{2,Int32}})
    addresolution!(ctx.budget)
    writer isa UnionSchema && return true              # Java's WriterUnion action is not an immediate error
    if reader isa UnionSchema
        return selectbranch(ctx, writer, reader, path).index != 0
    end
    typeof(writer) === typeof(reader) ||
        return kind(reader) in get(PROMOTIONS, kind(writer), ())
    writer isa Union{RecordSchema,ArraySchema,MapSchema} && return true
    writer isa FixedSchema &&
        return namesmatch(writer, reader::FixedSchema, ctx.budget) &&
               writer.size == reader.size && matchingdecimals(writer, reader)
    writer isa EnumSchema &&
        return namesmatch(writer, reader::EnumSchema, ctx.budget)
    return matchingdecimals(writer, reader)
end

function softrecordmatches(ctx::ResolveContext, writer::RecordSchema,
                           reader::RecordSchema,
                           path::BuildBuf{NTuple{2,Int32}})
    pair = (nodeid(writer), nodeid(reader))
    softpairpresent(path, pair, ctx.budget) && return true
    push!(path, ctx.budget, pair)
    try
        for (writerindex, field) in enumerate(writer.fields)
            readerindex = softclaim(ctx, writer, reader, writerindex)
            readerindex >= 0 || return false
            readerindex == 0 && continue
            softrootactionok(ctx, field.schema,
                             reader.fields[readerindex].schema, path) || return false
        end
        for (readerindex, field) in enumerate(reader.fields)
            candidate, _ = softcandidate(ctx, writer, field)
            candidate >= 0 || return false
            chosen = candidate != 0 &&
                     softclaim(ctx, writer, reader, candidate) == readerindex
            chosen && continue
            default = field.default
            default isa DefaultValue && default.valid || return false
        end
        return true
    finally
        path.len -= 1
    end
end

function selectbranch(ctx::ResolveContext, w::Schema, r::UnionSchema,
                      path::Union{Nothing,BuildBuf{NTuple{2,Int32}}}=nothing)
    if ctx.policy === :java
        for (j, c) in enumerate(r.branches)
            addresolution!(ctx.budget, 1)
            exactmatch(w, c, ctx.budget) && return BranchMatch(j, false)
        end
        if w isa RecordSchema
            ownedpath = path === nothing
            probe = ownedpath ? BuildBuf{NTuple{2,Int32}}(ctx.budget, 8) : path
            structurematch = 0
            try
                for (j, candidate) in enumerate(r.branches)
                    candidate isa RecordSchema || continue
                    addresolution!(ctx.budget)
                    softrecordmatches(ctx, w, candidate, probe::BuildBuf{NTuple{2,Int32}}) || continue
                    if structurematch == 0 || w.name.name == candidate.name.name
                        structurematch = j
                    end
                end
            finally
                if ownedpath
                    release!(ctx.budget,
                             vectorbytes(NTuple{2,Int32}, length((probe::BuildBuf).data)) +
                             shellbytes(typeof(probe)))
                end
            end
            structurematch != 0 && return BranchMatch(structurematch, true)
        end
    end
    for (j, c) in enumerate(r.branches)
        addresolution!(ctx.budget, 1)
        matches(w, c, ctx.budget) && return BranchMatch(j, false)
    end
    return BranchMatch(0, false)
end

"Snapshot which plain-plan memo slots existed before a fallible union-member attempt."
function plansnapshot(ctx::ResolveContext, memo::Vector{Union{Nothing,ReadPlan}})
    charge = vectorbytes(Bool, length(memo))
    reserve!(ctx.budget, charge)
    present = Vector{Bool}(undef, length(memo))
    allocated!(ctx.budget, charge)
    for i in eachindex(memo)
        present[i] = memo[i] !== nothing
    end
    return (present, charge)
end

"Drop plain plans first built by a failed branch, while preserving plans owned before the frame."
function restoreplans!(memo::Vector{Union{Nothing,ReadPlan}}, present::Vector{Bool})
    for i in eachindex(memo)
        present[i] || (memo[i] = nothing)
    end
    return nothing
end

"Roll back a failed branch but keep exact replacement growth in its reusable pair memo tables."
function rollbackbranch!(budget::Budget, checkpoint::NTuple{2,Int}, keep::Int)
    reserved0, pending0 = checkpoint
    pend = budget.pending - pending0
    pend > 0 && unreserve!(budget, pend)
    resident = budget.reserved - reserved0 - max(pend, 0)
    discard = resident - keep
    discard >= 0 || throw(ArgumentError("branch rollback retained $keep bytes from only $resident resident bytes"))
    discard > 0 && release!(budget, discard)
    return nothing
end

"Build a failed union branch from diagnostics already owned and charged by this resolution."
function unresolvablebranch(msg::String, wp::String, rp::String, skipper::ReadPlan, budget::Budget)
    return plannode(() -> UnresolvableBranch(msg, wp, rp, skipper), budget)
end

function resolvebranch(ctx::ResolveContext, w::Schema, r::Schema,
                       wp::ResolutionPath, rp::ResolutionPath,
                       javasoft::Bool=false)
    readerpresent, readercharge = plansnapshot(ctx, ctx.readermemo)
    writerpresent, writercharge = plansnapshot(ctx, ctx.writermemo)
    checkpoint = budgetcheckpoint(ctx.budget)
    tables0 = memotablebytes(ctx)
    paths0 = pathstorage(wp) + pathstorage(rp)
    try
        out = resolvenode(ctx, w, r, wp, rp, javasoft)
        release!(ctx.budget, readercharge + writercharge)
        return out
    catch e
        if !(e isa ResolutionError)
            release!(ctx.budget, readercharge + writercharge)
            rethrow()
        end
        restoreplans!(ctx.readermemo, readerpresent)
        restoreplans!(ctx.writermemo, writerpresent)
        memoclear!(ctx)
        diagnostics = stringbytes(sizeof(e.msg)) + stringbytes(sizeof(e.writerpath)) +
                      stringbytes(sizeof(e.readerpath))
        pathgrowth = pathstorage(wp) + pathstorage(rp) - paths0
        keep = memotablebytes(ctx) - tables0 + diagnostics + pathgrowth
        rollbackbranch!(ctx.budget, checkpoint, keep)
        release!(ctx.budget, readercharge + writercharge)
        skipper = writerplan(ctx, w)                  # built after rollback because the returned branch owns it
        return unresolvablebranch(e.msg, e.writerpath, e.readerpath, skipper, ctx.budget)
    end
end

function resolvewriterunion(ctx::ResolveContext, w::UnionSchema, r::Schema,
                            wp::ResolutionPath, rp::ResolutionPath,
                            javasoft::Bool=false)
    n = length(w.branches)
    vectors = vectorbytes(ReadPlan, n) + vectorbytes(Int, n)
    node = 64
    reserve!(ctx.budget, vectors + node)
    branches = Vector{ReadPlan}(undef, n)
    resize!(branches, 0)
    rindex = Vector{Int}(undef, n)
    resize!(rindex, 0)
    allocated!(ctx.budget, vectors)
    for (i, b) in enumerate(w.branches)
        pushindex!(wp, i - 1, ctx.budget)
        try
            if r isa UnionSchema
                selected = selectbranch(ctx, b, r)
                j = selected.index
                if j == 0
                    skipper = writerplan(ctx, b)
                    msg = diagnosticstring(ctx.budget, "writer union branch ",
                        SchemaDescription(b), " matches no reader branch")
                    writerpath = formatpath(wp, ctx.budget)
                    readerpath = formatpath(rp, ctx.budget)
                    push!(branches, unresolvablebranch(msg, writerpath, readerpath, skipper, ctx.budget))
                else
                    pushindex!(rp, j - 1, ctx.budget)
                    try
                        push!(branches, resolvebranch(ctx, b, r.branches[j], wp, rp,
                                                      javasoft || selected.softrecord))
                    finally
                        poppath!(rp)
                    end
                end
                push!(rindex, j)
            else
                push!(branches, resolvebranch(ctx, b, r, wp, rp, javasoft))
                push!(rindex, 0)
            end
        finally
            poppath!(wp)
        end
    end
    plan = UnionResolvePlan(branches, rindex, r isa UnionSchema ? nullablebranch(r) : 0, r isa UnionSchema)
    allocated!(ctx.budget, node)
    return plan
end

function resolvereaderunion(ctx::ResolveContext, w::Schema, r::UnionSchema,
                            wp::ResolutionPath, rp::ResolutionPath,
                            javasoft::Bool=false)
    selected = selectbranch(ctx, w, r)
    j = selected.index
    j == 0 && reserror(ctx, wp, rp, "writer ", SchemaDescription(w),
        " matches no branch of the reader union")
    pushindex!(rp, j - 1, ctx.budget)
    inner = try
        resolvenode(ctx, w, r.branches[j], wp, rp,
                    javasoft || selected.softrecord)
    finally
        poppath!(rp)
    end
    return plannode(() -> WrapPlan(inner, j, nullablebranch(r)), ctx.budget)
end

function resolveenum(ctx::ResolveContext, w::EnumSchema, r::EnumSchema,
                     wp::ResolutionPath, rp::ResolutionPath)
    w.symbols.data == r.symbols.data && return readerplan(ctx, r)
    checkpoint = budgetcheckpoint(ctx.budget)
    mapbytes = vectorbytes(Int32, length(w.symbols))
    node = 64
    try
        reserve!(ctx.budget, mapbytes + node)
        map = Vector{Int32}(undef, length(w.symbols))
        allocated!(ctx.budget, mapbytes)
        for (i, sym) in enumerate(w.symbols)
            addresolution!(ctx.budget, 1)
            map[i] = Int32(get(r.symbolindex, sym, 0))
        end
        ownedwp = formatpath(wp, ctx.budget)
        ownedrp = formatpath(rp, ctx.budget)
        d = r.default
        plan = EnumRemapPlan(w, r, map, (d isa DefaultValue && d.valid) ? Int32(d.index) : Int32(0),
                             ownedwp, ownedrp)
        allocated!(ctx.budget, node)
        return plan
    catch
        rollbackreservations!(ctx.budget, checkpoint)
        rethrow()
    end
end

function resolverecord(ctx::ResolveContext, w::RecordSchema, r::RecordSchema,
                       wp::ResolutionPath, rp::ResolutionPath,
                       javasoft::Bool=false)
    nw, nr = length(w.fields), length(r.fields)
    slots = vectorbytes(Pair{Int,ReadPlan}, nw) + vectorbytes(Pair{Int,DefaultPlan}, nr) + vectorbytes(Int, nr) + 64
    reserve!(ctx.budget, slots)                        # exact step/default/box capacity and node shell (§4.4)
    steps = Vector{Pair{Int,ReadPlan}}(undef, nw)
    resize!(steps, 0)
    defaults = Vector{Pair{Int,DefaultPlan}}(undef, nr)
    resize!(defaults, 0)
    boxes = Vector{Int}(undef, nr)
    resize!(boxes, 0)
    plan = ResolvedRecordPlan(r, steps, defaults, boxes)
    allocated!(ctx.budget, slots)
    memostore!(ctx, w, r, plan, javasoft)              # before the fields, so recursive references resolve
    candcharge = vectorbytes(Int, nr)
    aliascharge = vectorbytes(UInt64, cld(nr, 64)) + 64
    claimedcharge = vectorbytes(Int, nw)
    scratch = candcharge + aliascharge + claimedcharge
    reserve!(ctx.budget, scratch)                      # the matching scratch, released when the plan is filled
    settled = 0
    try
        cand = zeros(Int, nr)                          # the writer field each reader field names
        allocated!(ctx.budget, candcharge)
        settled += candcharge
        viaalias = falses(nr)
        allocated!(ctx.budget, aliascharge)
        settled += aliascharge
        for (j, rf) in enumerate(r.fields)
            for name in Iterators.flatten(((rf.name,), rf.aliases))
                addresolution!(ctx.budget, 1)
                i = budgetedget(w.fieldindex, name, 0, ctx.budget)
                (i == 0 || i == cand[j]) && continue
                if cand[j] != 0
                    pushfield!(rp, rf.name, ctx.budget)
                    try
                        reserror(ctx, wp, rp, "reader field \"", rf.name,
                            "\" matches more than one writer field")
                    finally
                        poppath!(rp)
                    end
                end
                cand[j] = i
                viaalias[j] = name != rf.name
            end
        end
        claimed = zeros(Int, nw)                       # reader field per writer field; aliases claim first
        allocated!(ctx.budget, claimedcharge)
        settled += claimedcharge
        for pass in (true, false), (j, rf) in enumerate(r.fields)
            (cand[j] != 0 && viaalias[j] == pass) || continue
            i = cand[j]
            if claimed[i] != 0
                if pass
                    pushfield!(wp, w.fields[i].name, ctx.budget)
                    try
                        reserror(ctx, wp, rp, "writer field \"", w.fields[i].name,
                            "\" is claimed by two reader field aliases")
                    finally
                        poppath!(wp)
                    end
                end
                cand[j] = 0                            # consumed by an alias: this reader field needs its default
                continue
            end
            claimed[i] = j
        end
        for (i, wf) in enumerate(w.fields)
            j = claimed[i]
            if j == 0
                push!(plan.steps, 0 => writerplan(ctx, wf.schema))
            else
                pushfield!(wp, wf.name, ctx.budget)
                pushfield!(rp, r.fields[j].name, ctx.budget)
                fieldplan = try
                    resolvenode(ctx, wf.schema, r.fields[j].schema, wp, rp,
                                javasoft)
                finally
                    poppath!(rp)
                    poppath!(wp)
                end
                push!(plan.steps, j => fieldplan)
            end
        end
        for (j, rf) in enumerate(r.fields)
            push!(plan.boxes, boxcharge(juliatype(rf.schema)))
            cand[j] == 0 || continue
            d = rf.default
            if !(d isa DefaultValue && d.valid)
                pushfield!(rp, rf.name, ctx.budget)
                try
                    reserror(ctx, wp, rp, "reader field \"", rf.name,
                        "\" has no writer field and no valid default")
                finally
                    poppath!(rp)
                end
            end
            work = defaultwork(rf.schema, d.json, d.branch, ctx.budget)
            default = plannode(() -> DefaultPlan(rf.schema, d.json, d.branch,
                                                 work.branches, work.values,
                                                 work.maxdepth),
                               ctx.budget)
            push!(plan.defaults, j => default)
        end
        return plan
    finally
        unsettled = scratch - settled
        unsettled > 0 && unreserve!(ctx.budget, unsettled)
        settled > 0 && release!(ctx.budget, settled)
    end
end

# ---- decoding and skipping --------------------------------------------------------------------------

function syntheticraw!(kind::Symbol, decoder::Decoder)
    kind === :int && return (readint(decoder); nothing)
    kind === :long && return (readlong(decoder); nothing)
    kind === :float && return (readfloat(decoder); nothing)
    kind === :double && return (readdouble(decoder); nothing)
    kind in (:string, :bytes) && return skiplen(decoder)
    throw(ArgumentError("unsupported promoted writer kind $kind"))
end

function fastskipvalues(plan::PromotePlan, limits::Limits)
    return plan.writer in (:string, :bytes) ? -1 : 1
end

function fastskipvalues(::DefaultPlan, limits::Limits)
    return 0
end

function fastskipvalues(::EnumRemapPlan, limits::Limits)
    return 1
end

function fastskipvalues(plan::UnresolvableBranch, limits::Limits)
    return fastskipvalues(plan.skipper, limits)
end

function fastskipvalues(plan::UnionResolvePlan, limits::Limits)
    isempty(plan.branches) && return -1
    branchvalues = fastskipvalues(first(plan.branches), limits)
    branchvalues >= 0 || return -1
    for index in 2:length(plan.branches)
        fastskipvalues(plan.branches[index], limits) == branchvalues || return -1
    end
    return checked_add(1, branchvalues)
end

function fastskipvalues(plan::WrapPlan, limits::Limits)
    return fastskipvalues(plan.inner, limits)
end

function fastskipvalues(::ResolvedRecordPlan, limits::Limits)
    return -1
end

function syntheticvalue!(plan::PromotePlan, decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    values = countwriter!(counter)
    syntheticraw!(plan.writer, decoder)
    return values
end

function syntheticvalue!(plan::DefaultPlan, decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    active || return 0
    checkdefaultdepth(plan, decoder)
    countsynthetic!(counter, plan.values)
    return plan.values
end

function syntheticvalue!(plan::EnumRemapPlan, decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    values = countwriter!(counter)
    readindex(decoder, length(plan.writer.symbols))
    return values
end

function syntheticvalue!(plan::UnresolvableBranch, decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    active && counter.countwriter &&
        throw(ResolutionError(plan.msg, plan.writerpath, plan.readerpath))
    return syntheticvalue!(plan.skipper, decoder, false, counter)
end

function syntheticvalue!(plan::UnionResolvePlan, decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    isempty(plan.branches) && dataerror(decoder, "empty union has no datum")
    values = countwriter!(counter)
    index = readindex(decoder, length(plan.branches))
    return syntheticvalueadd(counter, values,
                             syntheticvalue!(plan.branches[index], decoder,
                                             active, counter))
end

function syntheticvalue!(plan::WrapPlan, decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    return syntheticvalue!(plan.inner, decoder, active, counter)
end

function syntheticvalue!(plan::ResolvedRecordPlan, decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    enter!(decoder)
    values = countwriter!(counter)
    try
        for (slot, field) in plan.steps
            values = syntheticvalueadd(counter, values,
                                       syntheticvalue!(field, decoder,
                                                       active && slot != 0,
                                                       counter))
        end
        if active
            for (_, default) in plan.defaults
                checkdefaultdepth(default, decoder)
                countsynthetic!(counter, default.values)
                values = syntheticvalueadd(counter, values, default.values)
            end
        end
    finally
        leave!(decoder)
    end
    return values
end

function projectedsyntheticvalue!(plan::ResolvedRecordPlan, decoder::Decoder,
                                  projection::Projection,
                                  counter::SyntheticCounter)
    enter!(decoder)
    values = countwriter!(counter)
    try
        for (slot, field) in plan.steps
            selected = slot != 0 && projection.slots[slot] != 0
            values = syntheticvalueadd(counter, values,
                                       syntheticvalue!(field, decoder, selected,
                                                       counter))
        end
        for (slot, default) in plan.defaults
            projection.slots[slot] == 0 && continue
            checkdefaultdepth(default, decoder)
            countsynthetic!(counter, default.values)
            values = syntheticvalueadd(counter, values, default.values)
        end
    finally
        leave!(decoder)
    end
    return values
end

function checkdefaultdepth(plan::DefaultPlan, decoder::Decoder)
    plan.maxdepth == 0 && return nothing
    checkdepth(decoder.budget, satadd(decoder.depth, plan.maxdepth))
    return nothing
end

function readraw(kind::Symbol, d::Decoder)
    kind === :int && return readint(d)
    kind === :long && return readlong(d)
    kind === :float && return readfloat(d)
    kind === :double && return readdouble(d)
    kind === :string && return readstring(d)
    return readbytes(d)
end

function decodevalue(p::PromotePlan, d::Decoder)
    return fromraw(p.reader, d, readraw(p.writer, d))
end

function skipvalue(p::PromotePlan, d::Decoder)
    value = fromraw(p.reader, d, readraw(p.writer, d))
    if value isa String
        release!(d.budget, stringbytes(sizeof(value)))
    elseif value isa Vector{UInt8}
        release!(d.budget, bytesbytes(length(value)))
    elseif value isa WideDecimal
        release!(d.budget, storagebytes(value))
    end
    return nothing
end

# The reader's interpretation of a promoted raw value.
function fromraw(::LongPlan, d::Decoder, raw::Int32)
    return Int64(raw)
end

function fromraw(::FloatPlan, d::Decoder, raw::Union{Int32,Int64})
    return Float32(raw)
end

function fromraw(::DoublePlan, d::Decoder, raw::Union{Int32,Int64,Float32})
    return Float64(raw)
end

function fromraw(::TimeMicrosPlan, d::Decoder, raw::Int32)
    v = Int64(raw)
    0 <= v < 86_400_000_000 || dataerror(d, "time-micros value $v out of range")
    return Time(Nanosecond(v * 1_000))
end

function fromraw(::TimestampPlan{P}, d::Decoder, raw::Int32) where {P}
    return Timestamp{P}(Int64(raw))
end

function fromraw(::LocalTimestampPlan{P}, d::Decoder, raw::Int32) where {P}
    return LocalTimestamp{P}(Int64(raw))
end

function fromraw(::StringPlan, d::Decoder, raw::Vector{UInt8})
    validutf8(raw, 1, length(raw)) || dataerror(d, "invalid UTF-8 in string")
    return takeownedstring!(raw, d.budget)
end

function fromraw(::UUIDStringPlan, d::Decoder, raw::Vector{UInt8})
    validuuid(raw, 1, length(raw)) || dataerror(d, "invalid uuid string")
    u = uuidfrombuffer(raw, 1)
    release!(d.budget, bytesbytes(length(raw)))
    return u
end

function fromraw(::BytesPlan, d::Decoder, raw::String)
    n = sizeof(raw)
    reserve!(d.budget, bytesbytes(n))
    out = Vector{UInt8}(codeunits(raw))
    allocated!(d.budget, bytesbytes(n))
    release!(d.budget, stringbytes(n))                    # the promoted string dies with this frame
    return out
end

function fromraw(p::DecimalPlan, d::Decoder, raw::String)
    n = sizeof(raw)
    reserve!(d.budget, bytesbytes(n))
    bytes = Vector{UInt8}(codeunits(raw))
    allocated!(d.budget, bytesbytes(n))
    release!(d.budget, stringbytes(n))                    # the promoted string dies with this frame
    isempty(bytes) && dataerror(d, "empty decimal payload")
    v = decimalfrombytes(Decoder(bytes, d.budget), p, 1, n)
    release!(d.budget, bytesbytes(n))                     # the transient copy dies with this frame
    return v
end

function fromraw(p::ReadPlan, d::Decoder, raw)
    return dataerror(d, "internal error: no promotion of $(typeof(raw)) into $(typeof(p))")
end

function decodevalue(p::DefaultPlan, d::Decoder)
    return jsonvalue(p, d.budget; depth=satadd(d.depth, 1))
end

function skipvalue(::DefaultPlan, d::Decoder)
    return nothing
end

function decode(p::DefaultPlan, d::Decoder)
    return jsonvalue(p, d.budget; depth=satadd(d.depth, 1))
end

function skip(::DefaultPlan, d::Decoder)
    return nothing
end

function enumremapindex(p::EnumRemapPlan, d::Decoder)
    i = readindex(d, length(p.writer.symbols))
    j = p.map[i]
    if j == 0
        j = p.default
        j == 0 && throw(ResolutionError("writer enum symbol is absent from the reader and the reader has no default",
                                        p.writerpath, p.readerpath))
    end
    return j
end

function decodevalue(p::EnumRemapPlan, d::Decoder)
    j = enumremapindex(p, d)
    reserve!(d.budget, enumvaluebytes())
    v = EnumValue(p.reader, j, Val(:unchecked))
    allocated!(d.budget, enumvaluebytes())
    return v
end

function skipvalue(p::EnumRemapPlan, d::Decoder)
    enumremapindex(p, d)
    return nothing
end

function decodevalue(p::UnresolvableBranch, d::Decoder)
    throw(ResolutionError(p.msg, p.writerpath, p.readerpath))
end

function skipvalue(p::UnresolvableBranch, d::Decoder)
    throw(ResolutionError(p.msg, p.writerpath, p.readerpath))
end

function wrapreader(d::Decoder, v, j::Int, nullable::Int)
    nullable != 0 && return j == nullable ? missing : v
    box = isbits(v) ? boxbytes(typeof(v)) : 0
    reserve!(d.budget, unionvaluebytes() + box)
    u = UnionValue(j, v)
    allocated!(d.budget, unionvaluebytes() + box)
    return u
end

function decodevalue(p::UnionResolvePlan, d::Decoder)
    n = length(p.branches)
    n == 0 && dataerror(d, "empty union has no datum")
    i = readindex(d, n)
    v = decode(p.branches[i], d)
    p.readerunion || return v
    return wrapreader(d, v, p.readerindex[i], p.nullable)
end

function skipvalue(p::UnionResolvePlan, d::Decoder)
    n = length(p.branches)
    n == 0 && dataerror(d, "empty union has no datum")
    return skip(p.branches[readindex(d, n)], d)
end

function decodevalue(p::WrapPlan, d::Decoder)
    return wrapreader(d, decodevalue(p.inner, d), p.readerindex, p.nullable)
end

function skipvalue(p::WrapPlan, d::Decoder)
    return skipvalue(p.inner, d)
end

function decodevalue(p::ResolvedRecordPlan, d::Decoder)
    enter!(d)
    n = length(p.schema.fields)
    reserve!(d.budget, recordbytes(n))
    vals = Vector{Any}(undef, n)
    allocated!(d.budget, vectorbytes(Any, n))                # the Record shell settles at construction
    for (slot, plan) in p.steps
        if slot == 0
            skip(plan, d)
        else
            v = decode(plan, d)
            b = p.boxes[slot]
            b > 0 && reserve!(d.budget, b)
            vals[slot] = v                                   # an isbits value boxes on assignment
            b > 0 && allocated!(d.budget, b)
        end
    end
    for (slot, dp) in p.defaults
        v = jsonvalue(dp, d.budget; depth=satadd(d.depth, 1))   # a fresh value per record
        b = p.boxes[slot]
        b > 0 && reserve!(d.budget, b)
        vals[slot] = v
        b > 0 && allocated!(d.budget, b)
    end
    leave!(d)
    r = Record(p.schema, vals, Val(:unchecked))
    allocated!(d.budget, recordbytes(n) - vectorbytes(Any, n))
    return r
end

function skipvalue(p::ResolvedRecordPlan, d::Decoder)
    enter!(d)
    for (_, plan) in p.steps
        skip(plan, d)
    end
    leave!(d)
    return nothing
end

# ---- column builders over resolved records (plan §6; used by Avro.Table and Rows partitions) ---------

"The retained builder state for one resolved reader record, excluding typed column data."
function columnbuildersstate(p::ResolvedRecordPlan,
                             projection::Union{Nothing,Projection}=nothing)
    charge = vectorbytes(ColumnBuilder, length(p.schema.fields))
    for (slot, plan) in p.steps
        slot == 0 && continue
        if projection === nothing || projection.slots[slot] != 0
            E = juliatype(p.schema.fields[slot].schema)
            charge = checked_add(charge,
                                 columnnodebytes(TypedColumn{E,typeof(plan)}))
        else
            charge = checked_add(charge,
                                 columnnodebytes(SkipColumn{typeof(plan)}))
        end
    end
    for (slot, plan) in p.defaults
        if projection === nothing || projection.slots[slot] != 0
            E = juliatype(p.schema.fields[slot].schema)
            charge = checked_add(charge,
                                 columnnodebytes(TypedColumn{E,typeof(plan)}))
        else
            charge = checked_add(charge,
                                 columnnodebytes(SkipColumn{typeof(plan)}))
        end
    end
    return charge
end

"The resolved builder construction peak, including its temporary reader-slot plan vector."
function columnbuilderpeakstate(p::ResolvedRecordPlan,
                                projection::Union{Nothing,Projection})
    scratch = vectorbytes(Union{Nothing,ReadPlan}, length(p.schema.fields))
    return checked_add(columnbuildersstate(p, projection), scratch)
end

function decoderow!(cols::Vector{ColumnBuilder}, d::Decoder, ::RecordPlan)
    return decoderow!(cols, d)
end

"One resolved record row into reader-slot builders: writer-ordered steps, then the defaults."
function decoderow!(cols::Vector{ColumnBuilder}, d::Decoder, p::ResolvedRecordPlan)
    enter!(d)
    for (slot, plan) in p.steps
        if slot == 0 || cols[slot] isa SkipColumn
            skip(plan, d)
        else
            decodecell!(cols[slot]::TypedColumn, d)
        end
    end
    b = d.budget
    for (slot, dp) in p.defaults
        c = cols[slot]
        c isa SkipColumn && continue
        appendcell!(c::TypedColumn, b,
                    jsonvalue(dp, b; depth=satadd(d.depth, 1)))
    end
    leave!(d)
    return nothing
end

function columnbuilders(p::ResolvedRecordPlan, projection::Union{Nothing,Projection},
                        capacity::Int, budget::Budget)
    capacity >= 0 || throw(ArgumentError("capacity must be non-negative"))
    n = length(p.schema.fields)
    checkpoint = budgetcheckpoint(budget)
    plans = nothing
    cols = nothing
    try
        planbytes = vectorbytes(Union{Nothing,ReadPlan}, n)
        reserve!(budget, planbytes)
        plans = Vector{Union{Nothing,ReadPlan}}(nothing, n)
        allocated!(budget, planbytes)
        for (slot, plan) in p.steps
            slot == 0 || (plans[slot] = plan)
        end
        for (slot, dp) in p.defaults
            plans[slot] = dp
        end
        outer = vectorbytes(ColumnBuilder, n)
        reserve!(budget, outer)
        cols = Vector{ColumnBuilder}(undef, n)
        allocated!(budget, outer)
        for (i, f) in enumerate(p.schema.fields)
            plan = plans[i]::ReadPlan
            if projection === nothing || projection.slots[i] != 0
                cols[i] = makecolumn(juliatype(f.schema), plan, capacity, budget)
            else
                setskipcolumn!(cols, i, plan, budget)
            end
        end
        plans = nothing
        release!(budget, planbytes)
        return cols
    catch
        plans = nothing
        cols = nothing
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

function columnbuilders(p::ResolvedRecordPlan, selected::AbstractVector{Int}, capacity::Int,
                        budget::Budget)
    projection = positionalprojection(selected, length(p.schema.fields), budget)
    try
        return columnbuilders(p, projection, capacity, budget)
    finally
        release!(budget, projectionbytes(projection))
    end
end
