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
end

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

# ---- the resolver -----------------------------------------------------------------------------------

mutable struct ResolveContext
    const budget::Budget
    const policy::Symbol
    const memokeys::Vector{Vector{Int32}}          # per writer node id: sorted reader node ids
    const memoplans::Vector{Vector{ReadPlan}}
    const memocaps::Vector{Int32}                  # prebuilt capacity per slot (§4.4 replacement growth)
    const readermemo::Vector{Union{Nothing,ReadPlan}}
    const writermemo::Vector{Union{Nothing,ReadPlan}}
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
                 budget::Union{Nothing,Budget}=nothing)
    union_resolution in (:spec, :java) || throw(ArgumentError("union_resolution must be :spec or :java"))
    budget === nothing &&
        return withbudget(b -> resolve(writer, reader; union_resolution=union_resolution, limits=limits, budget=b), limits)
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
    ctx = ResolveContext(budget, union_resolution, memokeys, memoplans, zeros(Int32, wnodes),
                         Vector{Union{Nothing,ReadPlan}}(nothing, rnodes),
                         Vector{Union{Nothing,ReadPlan}}(nothing, wnodes))
    allocated!(budget, tables)
    plan = resolvenode(ctx, writer, reader, "\$", "\$")
    release!(budget, memotablebytes(ctx) + vectorbytes(Union{Nothing,ReadPlan}, rnodes) +
                     vectorbytes(Union{Nothing,ReadPlan}, wnodes))     # construction memos die here
    return ResolvedSchema(writer, reader, union_resolution, plan)
end

"""
    resolvingplan(writer, reader; union_resolution=:spec, limits=Limits()) -> ReadPlan

The read plan decoding data written with `writer` into values of `reader`.
"""
function resolvingplan(writer::Schema, reader::Schema; union_resolution::Symbol=:spec, limits::Limits=Limits(),
                       budget::Union{Nothing,Budget}=nothing)
    return resolve(writer, reader; union_resolution=union_resolution, limits=limits, budget=budget).plan
end

function reserror(msg::AbstractString, wp::AbstractString, rp::AbstractString)
    throw(ResolutionError(String(msg), String(wp), String(rp)))
end

function readerplan(ctx::ResolveContext, s::Schema)
    return readplan(s, ctx.readermemo, ctx.budget)
end

function writerplan(ctx::ResolveContext, s::Schema)
    return readplan(s, ctx.writermemo, ctx.budget)
end

function memoslot(ctx::ResolveContext, w::Schema)
    return Int(nodeid(w)) + 1                          # the tables are prebuilt over every writer node
end

function memolookup(ctx::ResolveContext, w::Schema, r::Schema)
    iw = memoslot(ctx, w)
    keys = ctx.memokeys[iw]
    ir = nodeid(r)
    i = searchsortedfirst(keys, ir)
    addresolution!(ctx.budget, 1 + (i <= length(keys) ? 1 : 0))
    (i <= length(keys) && keys[i] == ir) && return ctx.memoplans[iw][i]
    return nothing
end

function memostore!(ctx::ResolveContext, w::Schema, r::Schema, p::ReadPlan)
    iw = memoslot(ctx, w)
    keys = ctx.memokeys[iw]
    ir = nodeid(r)
    i = searchsortedfirst(keys, ir)
    if i <= length(keys) && keys[i] == ir
        ctx.memoplans[iw][i] = p
        return p
    end
    if length(keys) == ctx.memocaps[iw]
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
    insert!(keys, i, ir)
    insert!(ctx.memoplans[iw], i, p)
    addresolution!(ctx.budget, length(keys) - i + 1)
    return p
end

# A failed subtree (a writer union branch that does not resolve) leaves no half-built entries behind.
function memoclear!(ctx::ResolveContext)
    foreach(empty!, ctx.memokeys)
    foreach(empty!, ctx.memoplans)
    return nothing
end

function resolvenode(ctx::ResolveContext, w::Schema, r::Schema, wp::String, rp::String)
    addresolution!(ctx.budget, 1)
    cached = memolookup(ctx, w, r)
    cached === nothing || return cached
    budgetedschemaequal(w, r, ctx.budget) && return memostore!(ctx, w, r, readerplan(ctx, r))   # identical subtree: the plain reader plan
    return memostore!(ctx, w, r, resolvekinds(ctx, w, r, wp, rp))
end

function describe(s::NamedSchema)
    return string(kind(s), " ", fullname(s))
end

function describe(s::Schema)
    return string(kind(s))
end

"Named types match by normalised fullname, by a reader alias, or by the unqualified name (the spec's record rule)."
function namesmatch(w::NamedSchema, r::NamedSchema)
    return fullname(w) == fullname(r) || fullname(w) in r.aliases || w.name.name == r.name.name
end

function resolvekinds(ctx::ResolveContext, w::Schema, r::Schema, wp::String, rp::String)
    w isa UnionSchema && return resolvewriterunion(ctx, w, r, wp, rp)
    r isa UnionSchema && return resolvereaderunion(ctx, w, r, wp, rp)
    if r isa RecordSchema
        w isa RecordSchema || reserror("writer $(describe(w)) does not resolve to reader $(describe(r))", wp, rp)
        namesmatch(w, r) || reserror("writer $(describe(w)) does not match reader $(describe(r)) by name or alias", wp, rp)
        return resolverecord(ctx, w, r, wp, rp)
    end
    if r isa EnumSchema
        w isa EnumSchema || reserror("writer $(describe(w)) does not resolve to reader $(describe(r))", wp, rp)
        namesmatch(w, r) || reserror("writer $(describe(w)) does not match reader $(describe(r)) by name or alias", wp, rp)
        return resolveenum(ctx, w, r, wp, rp)
    end
    if r isa FixedSchema
        w isa FixedSchema || reserror("writer $(describe(w)) does not resolve to reader $(describe(r))", wp, rp)
        namesmatch(w, r) || reserror("writer $(describe(w)) does not match reader $(describe(r)) by name or alias", wp, rp)
        w.size == r.size || reserror("fixed sizes differ ($(w.size) vs $(r.size))", wp, rp)
        checkdecimal(w, r, wp, rp)
        return readerplan(ctx, r)
    end
    if r isa ArraySchema
        w isa ArraySchema || reserror("writer $(describe(w)) does not resolve to reader array", wp, rp)
        return ArrayPlan(resolvenode(ctx, w.items, r.items, wp * "[items]", rp * "[items]"), elementtype(r.items), minsize(w.items, ctx.budget))
    end
    if r isa MapSchema
        w isa MapSchema || reserror("writer $(describe(w)) does not resolve to reader map", wp, rp)
        return MapPlan(resolvenode(ctx, w.values, r.values, wp * "[values]", rp * "[values]"), elementtype(r.values), minsize(w.values, ctx.budget))
    end
    return resolveleaf(ctx, w, r, wp, rp)
end

const PROMOTIONS = Dict{Symbol,Tuple{Vararg{Symbol}}}(:int => (:long, :float, :double), :long => (:float, :double), :float => (:double,),
                                                      :string => (:bytes,), :bytes => (:string,))

function checkdecimal(w::Schema, r::Schema, wp::String, rp::String)
    lw, lr = logical(w), logical(r)
    (lw isa DecimalLogical && lr isa DecimalLogical && (lw.precision != lr.precision || lw.scale != lr.scale)) &&
        reserror("decimal precision/scale differ ($(lw.precision)/$(lw.scale) vs $(lr.precision)/$(lr.scale))", wp, rp)
    return nothing
end

function resolveleaf(ctx::ResolveContext, w::Schema, r::Schema, wp::String, rp::String)
    kw, kr = kind(w), kind(r)
    checkdecimal(w, r, wp, rp)
    kw == kr && return readerplan(ctx, r)                        # same encoding; the reader's interpretation wins
    kr in get(PROMOTIONS, kw, ()) || reserror("writer $(describe(w)) does not resolve to reader $(describe(r))", wp, rp)
    return PromotePlan(kw, readerplan(ctx, r))
end

"Whether writer `w` matches reader branch `c` at the top level (the spec's matching rules, with promotion)."
function matches(w::Schema, c::Schema)
    c isa UnionSchema && return false
    w isa NamedSchema && return typeof(w) === typeof(c) && namesmatch(w, c) && (!(w isa FixedSchema) || w.size == c.size)
    c isa NamedSchema && return false
    kw, kc = kind(w), kind(c)
    kw == kc && return true
    return kc in get(PROMOTIONS, kw, ())
end

"Apache Java's first pass: the same kind (named types by fullname), no promotion."
function exactmatch(w::Schema, c::Schema)
    typeof(w) === typeof(c) || return false
    w isa NamedSchema && return fullname(w) == fullname(c)
    return true
end

function selectbranch(ctx::ResolveContext, w::Schema, r::UnionSchema)
    if ctx.policy === :java
        for (j, c) in enumerate(r.branches)
            addresolution!(ctx.budget, 1)
            exactmatch(w, c) && return j
        end
    end
    for (j, c) in enumerate(r.branches)
        addresolution!(ctx.budget, 1)
        matches(w, c) && return j
    end
    return 0
end

function resolvebranch(ctx::ResolveContext, w::Schema, r::Schema, wp::String, rp::String)
    try
        return resolvenode(ctx, w, r, wp, rp)
    catch e
        e isa ResolutionError || rethrow()
        memoclear!(ctx)
        return UnresolvableBranch(e.msg, e.writerpath, e.readerpath, writerplan(ctx, w))
    end
end

function resolvewriterunion(ctx::ResolveContext, w::UnionSchema, r::Schema, wp::String, rp::String)
    n = length(w.branches)
    slots = vectorbytes(ReadPlan, n) + vectorbytes(Int, n) + 64
    reserve!(ctx.budget, slots)                        # the exact branch/index vectors and node shell (§4.4)
    branches = Vector{ReadPlan}(undef, n)
    resize!(branches, 0)
    rindex = Vector{Int}(undef, n)
    resize!(rindex, 0)
    allocated!(ctx.budget, slots)
    for (i, b) in enumerate(w.branches)
        bp = string(wp, "[", i - 1, "]")
        if r isa UnionSchema
            j = selectbranch(ctx, b, r)
            if j == 0
                push!(branches, UnresolvableBranch("writer union branch $(describe(b)) matches no reader branch", bp, rp, writerplan(ctx, b)))
            else
                push!(branches, resolvebranch(ctx, b, r.branches[j], bp, string(rp, "[", j - 1, "]")))
            end
            push!(rindex, j)
        else
            push!(branches, resolvebranch(ctx, b, r, bp, rp))
            push!(rindex, 0)
        end
    end
    return UnionResolvePlan(branches, rindex, r isa UnionSchema ? nullablebranch(r) : 0, r isa UnionSchema)
end

function resolvereaderunion(ctx::ResolveContext, w::Schema, r::UnionSchema, wp::String, rp::String)
    j = selectbranch(ctx, w, r)
    j == 0 && reserror("writer $(describe(w)) matches no branch of the reader union", wp, rp)
    return WrapPlan(resolvenode(ctx, w, r.branches[j], wp, string(rp, "[", j - 1, "]")), j, nullablebranch(r))
end

function resolveenum(ctx::ResolveContext, w::EnumSchema, r::EnumSchema, wp::String, rp::String)
    w.symbols.data == r.symbols.data && return readerplan(ctx, r)
    mapbytes = vectorbytes(Int32, length(w.symbols)) + 64
    reserve!(ctx.budget, mapbytes)                     # the remap table and node shell (§4.4)
    map = Vector{Int32}(undef, length(w.symbols))
    allocated!(ctx.budget, mapbytes)
    for (i, sym) in enumerate(w.symbols)
        addresolution!(ctx.budget, 1)
        map[i] = Int32(get(r.symbolindex, sym, 0))
    end
    d = r.default
    return EnumRemapPlan(w, r, map, (d isa DefaultValue && d.valid) ? Int32(d.index) : Int32(0), wp, rp)
end

function resolverecord(ctx::ResolveContext, w::RecordSchema, r::RecordSchema, wp::String, rp::String)
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
    memostore!(ctx, w, r, plan)                        # before the fields, so recursive references resolve
    scratch = vectorbytes(Int, nr) + vectorbytes(UInt64, cld(nr, 64)) + vectorbytes(Int, nw) + 64
    reserve!(ctx.budget, scratch)                      # the matching scratch, released when the plan is filled
    cand = zeros(Int, nr)                              # the writer field each reader field names
    viaalias = falses(nr)
    for (j, rf) in enumerate(r.fields)
        for name in Iterators.flatten(((rf.name,), rf.aliases))
            addresolution!(ctx.budget, 1)
            i = get(w.fieldindex, name, 0)
            (i == 0 || i == cand[j]) && continue
            cand[j] != 0 && reserror("reader field \"$(rf.name)\" matches more than one writer field", wp, string(rp, ".", rf.name))
            cand[j] = i
            viaalias[j] = name != rf.name
        end
    end
    claimed = zeros(Int, nw)                           # reader field per writer field; aliases claim first
    allocated!(ctx.budget, scratch)
    for pass in (true, false), (j, rf) in enumerate(r.fields)
        (cand[j] != 0 && viaalias[j] == pass) || continue
        i = cand[j]
        if claimed[i] != 0
            pass && reserror("writer field \"$(w.fields[i].name)\" is claimed by two reader field aliases", string(wp, ".", w.fields[i].name), rp)
            cand[j] = 0                                # consumed by an alias: this reader field needs its default
            continue
        end
        claimed[i] = j
    end
    for (i, wf) in enumerate(w.fields)
        j = claimed[i]
        fp = string(wp, ".", wf.name)
        push!(plan.steps, j == 0 ? (0 => writerplan(ctx, wf.schema)) : (j => resolvenode(ctx, wf.schema, r.fields[j].schema, fp, string(rp, ".", r.fields[j].name))))
    end
    for (j, rf) in enumerate(r.fields)
        push!(plan.boxes, boxcharge(juliatype(rf.schema)))
        cand[j] == 0 || continue
        d = rf.default
        (d isa DefaultValue && d.valid) || reserror("reader field \"$(rf.name)\" has no writer field and no valid default", wp, string(rp, ".", rf.name))
        push!(plan.defaults, j => DefaultPlan(rf.schema, d.json))
    end
    release!(ctx.budget, scratch)                      # the matching scratch dies here
    return plan
end

# ---- decoding and skipping --------------------------------------------------------------------------

function readraw(kind::Symbol, d::Decoder)
    kind === :int && return readint(d)
    kind === :long && return readlong(d)
    kind === :float && return readfloat(d)
    kind === :double && return readdouble(d)
    kind === :string && return readstring(d)
    return readbytes(d)
end

function skipraw(kind::Symbol, d::Decoder)
    kind === :int && return (readint(d); nothing)
    kind === :long && return (readlong(d); nothing)
    kind === :float && return (readfloat(d); nothing)
    kind === :double && return (readdouble(d); nothing)
    return skiplen(d)
end

function decodevalue(p::PromotePlan, d::Decoder)
    return fromraw(p.reader, d, readraw(p.writer, d))
end

function skipvalue(p::PromotePlan, d::Decoder)
    return skipraw(p.writer, d)
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
    n = length(raw)
    reserve!(d.budget, stringbytes(n))
    str = String(raw)                                     # steals the buffer; raw is consumed here
    allocated!(d.budget, stringbytes(n))
    release!(d.budget, bytesbytes(n))
    return str
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
    return jsonvalue(p.schema, p.json, d.budget)
end

function skipvalue(::DefaultPlan, d::Decoder)
    return nothing
end

function enumremapindex(p::EnumRemapPlan, d::Decoder)
    i = readindex(d, length(p.writer.symbols))
    j = p.map[i]
    if j == 0
        j = p.default
        j == 0 && throw(ResolutionError("writer enum symbol \"$(escapename(p.writer.symbols[i]))\" is not a symbol of reader $(describe(p.reader)), which has no default", p.writerpath, p.readerpath))
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
    return (readindex(d, length(p.writer.symbols)); nothing)
end

function decodevalue(p::UnresolvableBranch, d::Decoder)
    throw(ResolutionError(p.msg, p.writerpath, p.readerpath))
end

function skipvalue(p::UnresolvableBranch, d::Decoder)
    return skipvalue(p.skipper, d)
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
    v = decodevalue(p.branches[i], d)
    p.readerunion || return v
    return wrapreader(d, v, p.readerindex[i], p.nullable)
end

function skipvalue(p::UnionResolvePlan, d::Decoder)
    n = length(p.branches)
    n == 0 && dataerror(d, "empty union has no datum")
    return skipvalue(p.branches[readindex(d, n)], d)
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
        countvalues!(d.budget)
        v = jsonvalue(dp.schema, dp.json, d.budget)          # a fresh value per record
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
        countvalues!(b)
        appendcell!(c::TypedColumn, b, jsonvalue(dp.schema, dp.json, b))
    end
    leave!(d)
    return nothing
end

function columnbuilders(p::ResolvedRecordPlan, selected::Union{Nothing,AbstractVector{Int}}, capacity::Int, budget::Budget)
    capacity >= 0 || throw(ArgumentError("capacity must be non-negative"))
    n = length(p.schema.fields)
    plans = Vector{Union{Nothing,ReadPlan}}(nothing, n)
    for (slot, plan) in p.steps
        slot == 0 || (plans[slot] = plan)
    end
    for (slot, dp) in p.defaults
        plans[slot] = dp
    end
    cols = Vector{ColumnBuilder}(undef, n)
    for (i, f) in enumerate(p.schema.fields)
        if selected === nothing || i in selected
            cols[i] = makecolumn(juliatype(f.schema), plans[i], capacity, budget)
        else
            cols[i] = SkipColumn(plans[i])
        end
    end
    return cols
end
