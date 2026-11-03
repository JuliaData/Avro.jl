# Schema resolution: `resolve(writer, reader)` builds a writer-driven read plan whose values are the reader
# schema's generic values. Pairs are memoised on (writer, reader) node ids; unions follow the spec policy
# (the first matching reader branch) or Apache Java's; failures are `ResolutionError`s carrying both paths.

"""
    Avro.ResolvedSchema

The result of `Avro.resolve`: the writer and reader schemas, the union policy and the resolving read
plan (`plan`), which `DatumReader(writer; reader_schema=reader)` uses.
"""
struct ResolvedSchema
    writer::Schema
    reader::Schema
    union_resolution::Symbol
    plan::Plan
end

function Base.show(io::IO, r::ResolvedSchema)
    return print(io, "Avro.ResolvedSchema(", kind(r.writer), " → ", kind(r.reader), ", ", r.union_resolution, ")")
end

# ---- resolving plan nodes ---------------------------------------------------------------------------

# A promotable writer leaf decoded as its raw primitive and interpreted by the reader's leaf plan.
struct PromotePlan <: Plan
    writer::Symbol        # :int, :long, :float, :double, :bytes or :string
    reader::Plan
end

# A reader-only field: its default built once into the generic value model (`value`), copied afresh for
# every record.
struct DefaultPlan <: Plan
    schema::Schema
    json::Any
    branch::Int
    value::Any
end

function DefaultPlan(schema::Schema, json, branch::Int)
    value = defaultvalue(schema, json, branch)
    value === nothing && throw(ArgumentError("a default accepted by schema parsing does not match its schema"))
    return DefaultPlan(schema, json, branch, value)
end

# Writer enum positions remapped to the reader's (0 = absent: the reader default, else an error).
struct EnumRemapPlan <: Plan
    reader::EnumSchema
    map::Vector{Int32}
    default::Int32
    writerpath::String
    readerpath::String
end

# A writer union branch with no resolving reader branch: raises when a datum selects it, even when skipped.
struct UnresolvableBranch <: Plan
    msg::String
    writerpath::String
    readerpath::String
end

# A writer union resolved branch by branch into the reader's (bare) values.
struct UnionResolvePlan <: Plan
    branches::Vector{Plan}
    readerindex::Vector{Int}    # the reader branch per writer branch (0 for a non-union reader)
end

# A non-union writer resolved against the selected reader union branch.
struct WrapPlan <: Plan
    inner::Plan
    readerindex::Int
end

# A record resolved field by field: writer-ordered steps into reader slots (0 = skip), then the defaults.
mutable struct ResolvedRecordPlan <: Plan
    const schema::RecordSchema                   # the reader record
    const steps::Vector{Pair{Int,Plan}}
    const defaults::Vector{Pair{Int,DefaultPlan}}
end

function isresolving(@nospecialize(p::Plan))
    return p isa Union{PromotePlan,DefaultPlan,EnumRemapPlan,UnresolvableBranch,UnionResolvePlan,WrapPlan,ResolvedRecordPlan}
end

# ---- the resolver -----------------------------------------------------------------------------------

const MemoKey = Tuple{Int32,Int32,Bool}          # (writer node id, reader node id, structural record match)

struct ResolveContext
    policy::Symbol
    little::Bool
    memo::Dict{MemoKey,Plan}
    inserted::Vector{MemoKey}                    # memo insertions, undone when a union branch attempt fails
    softmatches::Dict{NTuple{2,Int32},Bool}      # Java's structural record matches decided or assumed so far
    softtrail::Vector{NTuple{2,Int32}}           # the matches that hold only while the attempts in progress do
    readermemo::Vector{Union{Nothing,Plan}}
    writermemo::Vector{Union{Nothing,Plan}}
end

"""
    Avro.resolve(writer::Schema, reader::Schema; union_resolution=:spec) -> ResolvedSchema

Resolve data written with `writer` for a `reader`: kinds, promotions
(`int→long/float/double`, `long→float/double`, `float→double`, `string↔bytes`), named types by fullname,
reader aliases or unqualified name, record fields by name and reader field aliases (an alias consumes the
writer field), reader-only fields from valid defaults, enum symbols by name with the reader default,
unions under `union_resolution=:spec` (the first matching reader branch, including promotions) or
`:java` (Java's choice, which prefers exact types and adds structural record matches). Values follow
the reader schema. A promotion to floating point can lose integer precision; use `:java` to prefer
an exact branch when one is present.
"""
function resolve(writer::Schema, reader::Schema; union_resolution::Symbol=:spec)
    plan = resolvingplan(writer, reader; union_resolution=union_resolution)
    return ResolvedSchema(writer, reader, union_resolution, plan)
end

# The read plan decoding data written with `writer` into values of `reader`.
function resolvingplan(writer::Schema, reader::Schema; union_resolution::Symbol=:spec, little::Bool=false)
    union_resolution in (:spec, :java) || throw(ArgumentError("union_resolution must be :spec or :java"))
    ctx = ResolveContext(union_resolution, little, Dict{MemoKey,Plan}(), MemoKey[],
                         Dict{NTuple{2,Int32},Bool}(), NTuple{2,Int32}[],
                         Vector{Union{Nothing,Plan}}(nothing, graphinfo(reader).nodes),
                         Vector{Union{Nothing,Plan}}(nothing, graphinfo(writer).nodes))
    return resolvenode(ctx, writer, reader, Union{String,Int,Symbol}[], Union{String,Int,Symbol}[])
end

function reserror(wp, rp, parts...)
    throw(ResolutionError(string(parts...), formatpath(wp), formatpath(rp)))
end

function readerplan(ctx::ResolveContext, s::Schema)
    return readplan(s, ctx.readermemo, ctx.little ? :little : :standard)
end

function writerplan(ctx::ResolveContext, s::Schema)
    return readplan(s, ctx.writermemo, ctx.little ? :little : :standard)
end

function memostore!(ctx::ResolveContext, key::MemoKey, p::Plan)
    haskey(ctx.memo, key) || push!(ctx.inserted, key)
    ctx.memo[key] = p
    return p
end

# Forget the memo entries inserted after `mark`: a failed branch attempt leaves no partially built plan,
# while entries cached before it, including its ancestors' in-progress record plans, stay so recursive
# references still terminate.
function memoforget!(ctx::ResolveContext, mark::Int)
    while length(ctx.inserted) > mark
        delete!(ctx.memo, pop!(ctx.inserted))
    end
    return nothing
end

function resolvenode(ctx::ResolveContext, w::Schema, r::Schema, wp, rp, javasoft::Bool=false)
    key = (nodeid(w), nodeid(r), javasoft)
    cached = get(ctx.memo, key, nothing)
    cached === nothing || return cached
    schemaequal(w, r) && return memostore!(ctx, key, readerplan(ctx, r))
    return memostore!(ctx, key, resolvekinds(ctx, w, r, wp, rp, javasoft))
end

# Named types match by normalised fullname, by a reader alias, or by the unqualified name (the spec's record rule).
function namematch(w::NamedSchema, r::NamedSchema)
    w.name == r.name && return true
    for alias in r.aliases
        fullname(w.name) == alias && return true
    end
    return w.name.name == r.name.name
end

function resolvekinds(ctx::ResolveContext, w::Schema, r::Schema, wp, rp, javasoft::Bool)
    w isa UnionSchema && return resolvewriterunion(ctx, w, r, wp, rp, javasoft)
    r isa UnionSchema && return resolvereaderunion(ctx, w, r, wp, rp, javasoft)
    if r isa RecordSchema
        w isa RecordSchema || reserror(wp, rp, "writer ", schemalabel(w), " does not resolve to reader ", schemalabel(r))
        javasoft || namematch(w, r) ||
            reserror(wp, rp, "writer ", schemalabel(w), " does not match reader ", schemalabel(r), " by name or alias")
        return resolverecord(ctx, w, r, wp, rp, javasoft)
    end
    if r isa EnumSchema
        w isa EnumSchema || reserror(wp, rp, "writer ", schemalabel(w), " does not resolve to reader ", schemalabel(r))
        namematch(w, r) ||
            reserror(wp, rp, "writer ", schemalabel(w), " does not match reader ", schemalabel(r), " by name or alias")
        return resolveenum(ctx, w, r, wp, rp)
    end
    if r isa FixedSchema
        w isa FixedSchema || reserror(wp, rp, "writer ", schemalabel(w), " does not resolve to reader ", schemalabel(r))
        namematch(w, r) ||
            reserror(wp, rp, "writer ", schemalabel(w), " does not match reader ", schemalabel(r), " by name or alias")
        w.size == r.size || reserror(wp, rp, "fixed sizes differ (", w.size, " vs ", r.size, ")")
        checkdecimal(w, r, wp, rp)
        return readerplan(ctx, r)
    end
    if r isa ArraySchema
        w isa ArraySchema || reserror(wp, rp, "writer ", schemalabel(w), " does not resolve to reader array")
        push!(wp, :items)
        push!(rp, :items)
        items = resolvenode(ctx, w.items, r.items, wp, rp, javasoft)
        pop!(rp)
        pop!(wp)
        return ArrayPlan(items, elementtype(r.items), minsize(w.items))
    end
    if r isa MapSchema
        w isa MapSchema || reserror(wp, rp, "writer ", schemalabel(w), " does not resolve to reader map")
        push!(wp, :values)
        push!(rp, :values)
        values = resolvenode(ctx, w.values, r.values, wp, rp, javasoft)
        pop!(rp)
        pop!(wp)
        return MapPlan(values, elementtype(r.values), minsize(w.values))
    end
    return resolveleaf(ctx, w, r, wp, rp)
end

const PROMOTIONS = Dict{Symbol,Tuple{Vararg{Symbol}}}(:int => (:long, :float, :double), :long => (:float, :double), :float => (:double,),
                                                      :string => (:bytes,), :bytes => (:string,))

function checkdecimal(w::Schema, r::Schema, wp, rp)
    lw, lr = logical(w), logical(r)
    (lw isa DecimalLogical && lr isa DecimalLogical && (lw.precision != lr.precision || lw.scale != lr.scale)) &&
        reserror(wp, rp, "decimal precision/scale differ (", lw.precision, "/", lw.scale, " vs ", lr.precision, "/", lr.scale, ")")
    return nothing
end

function resolveleaf(ctx::ResolveContext, w::Schema, r::Schema, wp, rp)
    kw, kr = kind(w), kind(r)
    checkdecimal(w, r, wp, rp)
    kw == kr && return readerplan(ctx, r)                        # same encoding; the reader's interpretation wins
    kr in get(PROMOTIONS, kw, ()) || reserror(wp, rp, "writer ", schemalabel(w), " does not resolve to reader ", schemalabel(r))
    return PromotePlan(kw, readerplan(ctx, r))
end

# Whether writer `w` matches reader branch `c` at the top level (the spec's matching rules, with promotion).
function matches(w::Schema, c::Schema)
    c isa UnionSchema && return false
    w isa NamedSchema && return typeof(w) === typeof(c) && namematch(w, c) &&
        (!(w isa FixedSchema) || w.size == c.size) && matchingdecimals(w, c)
    c isa NamedSchema && return false
    kw, kc = kind(w), kind(c)
    kw == kc && return matchingdecimals(w, c)
    return kc in get(PROMOTIONS, kw, ())
end

# Recognized decimal schemas match only when both precision and scale match.
function matchingdecimals(w::Schema, r::Schema)
    lw = logical(w)
    lr = logical(r)
    lw isa DecimalLogical && lr isa DecimalLogical || return true
    return lw.precision == lr.precision && lw.scale == lr.scale
end

# Apache Java's first pass: the same kind (named types by fullname or reader alias), no promotion.
function exactmatch(w::Schema, c::Schema)
    typeof(w) === typeof(c) || return false
    if w isa NamedSchema
        w.name == c.name && return true
        for alias in c.aliases
            fullname(w.name) == alias && return true
        end
        return false
    end
    return true
end

# The writer field a reader field refers to (by name or alias; -1 when it refers to two) and whether via an alias.
function softcandidate(writer::RecordSchema, field::Field)
    candidate = 0
    viaalias = false
    for name in Iterators.flatten(((field.name,), field.aliases))
        index = get(writer.fieldindex, Symbol(name), 0)
        (index == 0 || index == candidate) && continue
        candidate == 0 || return (-1, false)
        candidate = index
        viaalias = name != field.name
    end
    return (candidate, viaalias)
end

# The reader field that claims writer field `writerindex` (aliases first; 0 for none, -1 when ambiguous).
function softclaim(writer::RecordSchema, reader::RecordSchema, writerindex::Int)
    aliasclaim = 0
    nameclaim = 0
    for (readerindex, field) in enumerate(reader.fields)
        candidate, viaalias = softcandidate(writer, field)
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

# Whether Java's resolver would accept the writer/reader pair without an immediate error.
function softrootactionok(ctx::ResolveContext, writer::Schema, reader::Schema)
    writer isa UnionSchema && return true              # Java's WriterUnion action is not an immediate error
    reader isa UnionSchema && return first(readerbranch(ctx, writer, reader)) != 0
    typeof(writer) === typeof(reader) || return kind(reader) in get(PROMOTIONS, kind(writer), ())
    writer isa Union{RecordSchema,ArraySchema,MapSchema} && return true
    writer isa FixedSchema &&
        return namematch(writer, reader::FixedSchema) && writer.size == reader.size && matchingdecimals(writer, reader)
    writer isa EnumSchema && return namematch(writer, reader::EnumSchema)
    return matchingdecimals(writer, reader)
end

# Java's structural record match: every claimed field pair resolves and every unclaimed reader field has a
# default. A pair is assumed to match while it is checked, so recursive records terminate; a failure also
# drops what was concluded under that assumption. Each pair is decided once, however many union paths reach it.
function softrecordmatches(ctx::ResolveContext, writer::RecordSchema, reader::RecordSchema)
    pair = (nodeid(writer), nodeid(reader))
    known = get(ctx.softmatches, pair, nothing)
    known === nothing || return known
    mark = length(ctx.softtrail)
    ctx.softmatches[pair] = true
    push!(ctx.softtrail, pair)
    softfieldsmatch(ctx, writer, reader) && return true
    while length(ctx.softtrail) > mark
        delete!(ctx.softmatches, pop!(ctx.softtrail))
    end
    ctx.softmatches[pair] = false
    return false
end

function softfieldsmatch(ctx::ResolveContext, writer::RecordSchema, reader::RecordSchema)
    for (writerindex, field) in enumerate(writer.fields)
        readerindex = softclaim(writer, reader, writerindex)
        readerindex >= 0 || return false
        readerindex == 0 && continue
        softrootactionok(ctx, field.schema, reader.fields[readerindex].schema) || return false
    end
    for (readerindex, field) in enumerate(reader.fields)
        candidate, _ = softcandidate(writer, field)
        candidate >= 0 || return false
        (candidate != 0 && softclaim(writer, reader, candidate) == readerindex) && continue
        field.default isa DefaultValue && field.default.valid || return false
    end
    return true
end

# The reader union branch writer `w` resolves to (0 for none) and whether it is Java's structural record match.
function readerbranch(ctx::ResolveContext, w::Schema, r::UnionSchema)
    if ctx.policy === :java
        for (j, c) in enumerate(r.branches)
            exactmatch(w, c) && return (j, false)
        end
        if w isa RecordSchema
            structural = 0
            for (j, c) in enumerate(r.branches)
                c isa RecordSchema && softrecordmatches(ctx, w, c) || continue
                (structural == 0 || w.name.name == c.name.name) && (structural = j)
            end
            structural == 0 || return (structural, true)
        end
    end
    for (j, c) in enumerate(r.branches)
        matches(w, c) && return (j, false)
    end
    return (0, false)
end

# Resolve one writer union branch speculatively: a `ResolutionError` becomes an `UnresolvableBranch`, the
# memo entries the attempt inserted are forgotten and the paths are restored.
function resolvebranch(ctx::ResolveContext, w::Schema, r::Schema, wp, rp, javasoft::Bool)
    mark = length(ctx.inserted)
    nw, nr = length(wp), length(rp)
    try
        return resolvenode(ctx, w, r, wp, rp, javasoft)
    catch e
        e isa ResolutionError || rethrow()
        memoforget!(ctx, mark)
        resize!(wp, nw)
        resize!(rp, nr)
        return UnresolvableBranch(e.msg, e.writerpath, e.readerpath)
    end
end

function resolvewriterunion(ctx::ResolveContext, w::UnionSchema, r::Schema, wp, rp, javasoft::Bool)
    branches = Plan[]
    rindex = Int[]
    for (i, b) in enumerate(w.branches)
        push!(wp, i - 1)
        if r isa UnionSchema
            j, structural = readerbranch(ctx, b, r)
            if j == 0
                push!(branches, UnresolvableBranch(string("writer union branch ", schemalabel(b), " matches no reader branch"),
                                                   formatpath(wp), formatpath(rp)))
            else
                push!(rp, j - 1)
                push!(branches, resolvebranch(ctx, b, r.branches[j], wp, rp, javasoft || structural))
                pop!(rp)
            end
            push!(rindex, j)
        else
            push!(branches, resolvebranch(ctx, b, r, wp, rp, javasoft))
            push!(rindex, 0)
        end
        pop!(wp)
    end
    return UnionResolvePlan(branches, rindex)
end

function resolvereaderunion(ctx::ResolveContext, w::Schema, r::UnionSchema, wp, rp, javasoft::Bool)
    j, structural = readerbranch(ctx, w, r)
    j == 0 && reserror(wp, rp, "writer ", schemalabel(w), " matches no branch of the reader union")
    push!(rp, j - 1)
    inner = resolvenode(ctx, w, r.branches[j], wp, rp, javasoft || structural)
    pop!(rp)
    return WrapPlan(inner, j)
end

function resolveenum(ctx::ResolveContext, w::EnumSchema, r::EnumSchema, wp, rp)
    w.symbols == r.symbols && return readerplan(ctx, r)
    map = Int32[get(r.symbolindex, sym, 0) for sym in w.symbols]
    d = r.default
    default = (d isa DefaultValue && d.valid) ? Int32(d.index) : Int32(0)
    return EnumRemapPlan(r, map, default, formatpath(wp), formatpath(rp))
end

function resolverecord(ctx::ResolveContext, w::RecordSchema, r::RecordSchema, wp, rp, javasoft::Bool)
    plan = ResolvedRecordPlan(r, Pair{Int,Plan}[], Pair{Int,DefaultPlan}[])
    memostore!(ctx, (nodeid(w), nodeid(r), javasoft), plan)    # before the fields, so recursive references resolve
    cand = zeros(Int, length(r.fields))                         # the writer field each reader field refers to
    viaalias = falses(length(r.fields))
    for (j, rf) in enumerate(r.fields)
        for name in Iterators.flatten(((rf.name,), rf.aliases))
            i = get(w.fieldindex, Symbol(name), 0)
            (i == 0 || i == cand[j]) && continue
            if cand[j] != 0
                push!(rp, rf.name)
                reserror(wp, rp, "reader field \"", rf.name, "\" matches more than one writer field")
            end
            cand[j] = i
            viaalias[j] = name != rf.name
        end
    end
    claimed = zeros(Int, length(w.fields))                      # reader field per writer field; aliases claim first
    for pass in (true, false), (j, rf) in enumerate(r.fields)
        (cand[j] != 0 && viaalias[j] == pass) || continue
        i = cand[j]
        if claimed[i] != 0
            if pass
                push!(wp, w.fields[i].name)
                reserror(wp, rp, "writer field \"", w.fields[i].name, "\" is claimed by two reader field aliases")
            end
            cand[j] = 0                                         # consumed by an alias: this reader field needs its default
            continue
        end
        claimed[i] = j
    end
    for (i, wf) in enumerate(w.fields)
        j = claimed[i]
        if j == 0
            push!(plan.steps, 0 => writerplan(ctx, wf.schema))
        else
            push!(wp, wf.name)
            push!(rp, r.fields[j].name)
            push!(plan.steps, j => resolvenode(ctx, wf.schema, r.fields[j].schema, wp, rp, javasoft))
            pop!(rp)
            pop!(wp)
        end
    end
    for (j, rf) in enumerate(r.fields)
        cand[j] == 0 || continue
        d = rf.default
        if !(d isa DefaultValue && d.valid)
            push!(rp, rf.name)
            reserror(wp, rp, "reader field \"", rf.name, "\" has no writer field and no valid default")
        end
        push!(plan.defaults, j => DefaultPlan(rf.schema, d.json, d.branch))
    end
    return plan
end

# ---- reader defaults --------------------------------------------------------------------------------

# The generic value of default JSON `j` under `s` (`branch` is the selected branch of a union `s`), or
# `nothing` when `j` does not match. Nested unions take their first matching branch and absent record
# members their field's default, as default validation does.
function defaultvalue(s::Schema, j, branch::Int)
    branch == 0 && return defaultvalue(s, j)
    return defaultvalue((s::UnionSchema).branches[branch], j)
end

function defaultvalue(s::UnionSchema, j)
    for b in s.branches
        v = defaultvalue(b, j)
        v === nothing || return v
    end
    return nothing
end

function defaultvalue(::NullSchema, j)
    return j === nothing ? missing : nothing
end

function defaultvalue(::BooleanSchema, j)
    return j isa Bool ? j : nothing
end

function defaultvalue(s::IntSchema, j)
    (j isa Int64 && typemin(Int32) <= j <= typemax(Int32)) || return nothing
    s.logical isa DateLogical && return DATE_EPOCH + Day(j)
    s.logical isa TimeMillis || return Int32(j)
    return 0 <= j < 86_400_000 ? Time(Nanosecond(j * 1_000_000)) : nothing
end

function defaultvalue(s::LongSchema, j)
    j isa Int64 || return nothing
    l = s.logical
    l isa TimeMicros && return 0 <= j < 86_400_000_000 ? Time(Nanosecond(j * 1_000)) : nothing
    l isa TimestampMillis && return timestamp(Millisecond, j)
    l isa TimestampMicros && return timestamp(Microsecond, j)
    l isa TimestampNanos && return timestamp(Nanosecond, j)
    l isa LocalTimestampMillis && return localtimestamp(Millisecond, j)
    l isa LocalTimestampMicros && return localtimestamp(Microsecond, j)
    l isa LocalTimestampNanos && return localtimestamp(Nanosecond, j)
    return j
end

function defaultvalue(::FloatSchema, j)
    return defaultfloat(Float32, j)
end

function defaultvalue(::DoubleSchema, j)
    return defaultfloat(Float64, j)
end

function defaultfloat(::Type{T}, j) where {T}
    j isa Union{Int64,Float64} && return T(j)
    j isa JSONNumber && return parsefloat(T, j.text)
    j == "NaN" && return T(NaN)
    j == "Infinity" && return T(Inf)
    j == "-Infinity" && return T(-Inf)
    return nothing
end

# The bytes of a `bytes`/`fixed` default: a string of code points ≤ U+00FF.
function defaultbytes(j)
    (j isa String && isstrictutf8(j) && all(c -> UInt32(c) <= 0xff, j)) || return nothing
    return UInt8[UInt8(c) for c in j]
end

function defaultvalue(s::BytesSchema, j)
    bytes = defaultbytes(j)
    (bytes === nothing || !(s.logical isa DecimalLogical)) && return bytes
    return defaultdecimal(s.logical, bytes)
end

function defaultvalue(s::FixedSchema, j)
    bytes = defaultbytes(j)
    (bytes === nothing || length(bytes) != s.size) && return nothing
    l = s.logical
    l isa DecimalLogical && return defaultdecimal(l, bytes)
    l isa UUIDLogical && return decodevalue(UUIDFixedPlan(), Decoder(bytes))
    l isa DurationLogical && return decodevalue(DurationPlan(), Decoder(bytes))
    return Fixed(s, bytes, Val(:unchecked))
end

function defaultdecimal(l::DecimalLogical, bytes::Vector{UInt8})
    isempty(bytes) && return nothing
    try
        return decimalfrombytes(Decoder(bytes), DecimalPlan(0, l.precision, l.scale, l.precision > 38), 1, length(bytes))
    catch e
        e isa DataError || rethrow()
        return nothing
    end
end

function defaultvalue(s::StringSchema, j)
    (j isa String && isstrictutf8(j)) || return nothing
    return s.logical isa UUIDLogical ? tryparse(UUID, j) : j
end

function defaultvalue(s::EnumSchema, j)
    i = j isa String ? get(s.symbolindex, j, 0) : 0
    return i == 0 ? nothing : EnumValue(s, Int32(i), Val(:unchecked))
end

function defaultvalue(s::ArraySchema, j)
    j isa AbstractVector || return nothing
    out = Vector{elementtype(s.items)}(undef, length(j))
    for (i, x) in enumerate(j)
        v = defaultvalue(s.items, x)
        v === nothing && return nothing
        out[i] = v
    end
    return out
end

function defaultvalue(s::MapSchema, j)
    j isa AbstractDict || return nothing
    out = Dict{String,elementtype(s.values)}()
    for (k, x) in j
        isstrictutf8(k) || return nothing
        v = defaultvalue(s.values, x)
        v === nothing && return nothing
        out[k] = v
    end
    return out
end

function defaultvalue(s::RecordSchema, j)
    j isa Props || return nothing
    members = memberlookup(j)
    vals = Vector{Any}(undef, length(s.fields))
    for (i, f) in enumerate(s.fields)
        d = f.default
        v = haskey(members, f.name) ? defaultvalue(f.schema, members[f.name]) :
            d isa DefaultValue ? defaultvalue(f.schema, d.json, d.branch) : nothing
        v === nothing && return nothing
        vals[i] = v
    end
    return Record(s, vals, Val(:unchecked))
end

# One reader default for one record: a fresh copy of the plan's value. Defaults come from the caller's reader
# schema, not the file, so they do not count against the file's value cap.
function freshvalue(v)
    return v
end

function freshvalue(v::Vector{UInt8})
    return copy(v)
end

function freshvalue(v::Fixed)
    return Fixed(v.schema, copy(v.bytes), Val(:unchecked))
end

function freshvalue(v::Vector)
    return map!(freshvalue, similar(v), v)
end

function freshvalue(v::Dict{String})
    out = sizehint!(empty(v), length(v))
    for (k, x) in v
        out[k] = freshvalue(x)
    end
    return out
end

function freshvalue(r::Record)
    return Record(getfield(r, :schema), Any[freshvalue(x) for x in getfield(r, :values)], Val(:unchecked))
end

# ---- decoding and skipping --------------------------------------------------------------------------

# The generic value of a resolving plan (`decodevalue`'s chain continued).
function decoderesolving(@nospecialize(p::Plan), d::Decoder)
    p isa ResolvedRecordPlan && return decoderesolvedrecord(p, d)
    p isa WrapPlan && return decodevalue(p.inner, d)
    p isa UnionResolvePlan && return decode(p.branches[readindex(d, unionsize(p, d))], d)
    p isa PromotePlan && return decodepromoted(p, d)
    p isa EnumRemapPlan && return EnumValue(p.reader, enumremapindex(p, d), Val(:unchecked))
    p isa DefaultPlan && return freshvalue(p.value)
    p isa UnresolvableBranch && throw(ResolutionError(p.msg, p.writerpath, p.readerpath))
    p isa WholeRecordPlan && return decodevalue(p.plan, d)
    throw(ArgumentError("internal error: a plan without a decoder"))
end

function unionsize(p::UnionResolvePlan, d::Decoder)
    n = length(p.branches)
    n == 0 && dataerror(d, "empty union has no datum")
    return n
end

# A promoted writer leaf in the generic value of the reader's leaf plan.
function decodepromoted(p::PromotePlan, d::Decoder)
    r = p.reader
    r isa LongPlan && return promotedvalue(Int64, p, d)
    r isa DoublePlan && return promotedvalue(Float64, p, d)
    r isa FloatPlan && return promotedvalue(Float32, p, d)
    r isa StringPlan && return promotedvalue(String, p, d)
    r isa BytesPlan && return promotedvalue(Vector{UInt8}, p, d)
    r isa UUIDStringPlan && return promotedvalue(UUID, p, d)
    r isa TimeMicrosPlan && return promotedvalue(Time, p, d)
    r isa DecimalPlan && return r.wide ? promotedvalue(WideDecimal, p, d) : promotedvalue(Decimal, p, d)
    r isa TimestampPlan{Millisecond} && return promotedvalue(Timestamp{Millisecond}, p, d)
    r isa TimestampPlan{Microsecond} && return promotedvalue(Timestamp{Microsecond}, p, d)
    r isa TimestampPlan{Nanosecond} && return promotedvalue(Timestamp{Nanosecond}, p, d)
    r isa LocalTimestampPlan{Millisecond} && return promotedvalue(LocalTimestamp{Millisecond}, p, d)
    r isa LocalTimestampPlan{Microsecond} && return promotedvalue(LocalTimestamp{Microsecond}, p, d)
    r isa LocalTimestampPlan{Nanosecond} && return promotedvalue(LocalTimestamp{Nanosecond}, p, d)
    return dataerror(d, "internal error: a promotion into a plan without one")
end

# A promoted writer leaf decoded as its raw primitive and interpreted as the reader's leaf type `T` (the
# generic value type of the reader's leaf plan).
function promotedvalue(::Type{T}, p::PromotePlan, d::Decoder) where {T}
    w = p.writer
    if T === Int64
        w === :int && return Int64(readint(d))
    elseif T === Float32
        w === :int && return Float32(readint(d))
        w === :long && return Float32(readlong(d))
    elseif T === Float64
        w === :int && return Float64(readint(d))
        w === :long && return Float64(readlong(d))
        w === :float && return Float64(readfloat(d))
    elseif T === Time
        w === :int && return timemicros(d, Int64(readint(d)))
    elseif T <: Union{Timestamp,LocalTimestamp}
        w === :int && return promotedtimestamp(T, readint(d))
    elseif T === String
        w === :bytes && return utf8string(d, readbytes(d))
    elseif T === UUID
        w === :bytes && return uuidfrombytes(d, readbytes(d))
    elseif T === Vector{UInt8}
        w === :string && return Vector{UInt8}(codeunits(readstring(d)))
    elseif T === Decimal || T === WideDecimal
        r = p.reader
        w === :string && r isa DecimalPlan && return decimalfromstring(d, r, readstring(d))::T
    end
    return dataerror(d, "internal error: a promotion its reader type does not take")
end

function promotedtimestamp(::Type{Timestamp{P}}, raw::Int32) where {P}
    return timestamp(P, raw)
end

function promotedtimestamp(::Type{LocalTimestamp{P}}, raw::Int32) where {P}
    return localtimestamp(P, raw)
end

function utf8string(d::Decoder, raw::Vector{UInt8})
    isstrictutf8(raw) || dataerror(d, "invalid UTF-8 in string")
    return String(raw)
end

function uuidfrombytes(d::Decoder, raw::Vector{UInt8})
    validuuid(raw, 1, length(raw)) || dataerror(d, "invalid uuid string")
    return uuidfrombuffer(raw, 1)
end

function decimalfromstring(d::Decoder, p::DecimalPlan, raw::String)
    bytes = Vector{UInt8}(codeunits(raw))
    isempty(bytes) && dataerror(d, "empty decimal payload")
    return decimalfrombytes(Decoder(bytes, d.limits), p, 1, length(bytes))
end

function enumremapindex(p::EnumRemapPlan, d::Decoder)
    j = p.map[readindex(d, length(p.map))]
    j == 0 || return j
    p.default == 0 && throw(ResolutionError("writer enum symbol is absent from the reader and the reader has no default",
                                            p.writerpath, p.readerpath))
    return p.default
end

function skipresolvedunion(p::UnionResolvePlan, d::Decoder)
    return skip(p.branches[readindex(d, unionsize(p, d))], d)
end

function decoderesolvedrecord(p::ResolvedRecordPlan, d::Decoder)
    enter!(d)
    vals = Vector{Any}(undef, length(p.schema.fields))
    for (slot, plan) in p.steps
        if slot == 0
            skip(plan, d)
        else
            vals[slot] = decode(plan, d)
        end
    end
    for (slot, dp) in p.defaults
        vals[slot] = freshvalue(dp.value)
    end
    leave!(d)
    return Record(p.schema, vals, Val(:unchecked))
end

function skipresolved(p::ResolvedRecordPlan, d::Decoder)
    enter!(d)
    for (_, plan) in p.steps
        skip(plan, d)
    end
    leave!(d)
    return nothing
end
