# Sort order (plan §4.12): `comparebytes` orders two encoded datums under a schema without materialising
# them — numerics with Java's `Double.compare` policy (NaN greatest and equal to itself, `-0.0 < 0.0`),
# bytes/fixed/strings unsigned lexicographic, arrays item-wise regardless of block form, enums by
# position, unions by branch then value, records by field order honouring `ascending`/`descending`/
# `ignore`, logical types by their underlying encoding; maps are unorderable outside `ignore` fields.
# `compare` orders Julia values through their canonical encodings, so the two agree for every
# Avro.jl-produced encoding.

"""
    Avro.comparebytes(schema, abytes, bbytes; limits=Limits(), validate=:strict) -> Int

`-1`, `0` or `1` ordering two encoded datums under `schema` (plan §4.12) without decoding them; each
buffer must hold exactly one datum (`DataError` otherwise). Maps are unorderable unless inside an
`order: "ignore"` field (`ArgumentError`).
"""
function comparebytes(s::Schema, a::AbstractVector{UInt8}, b::AbstractVector{UInt8}; limits::Limits=Limits(), validate::Symbol=:strict)
    validate in (:strict, :fast) || throw(ArgumentError("validate must be :strict or :fast"))
    checkorderable(s)
    return withbudget(limits) do budget
        abuf = sourcebytes(a, limits.max_datum_bytes, budget, DataError)
        bbuf = sourcebytes(b, limits.max_datum_bytes, budget, DataError)
        addinput!(budget, length(abuf) + length(bbuf))
        da = Decoder(abuf, budget; validate=validate)
        db = Decoder(bbuf, budget; validate=validate)
        c = comparevalue(s, readplan(s; budget=budget), da, db)
        da.pos == length(abuf) + 1 || throw(DataError("trailing bytes after the first datum", da.pos))
        db.pos == length(bbuf) + 1 || throw(DataError("trailing bytes after the second datum", db.pos))
        return c
    end
end

"""
    Avro.compare(schema, a, b; limits=Limits()) -> Int

Order two Julia values under `schema` by comparing their canonical encodings (`comparebytes` of
`encode(schema, x)`); cyclic or over-limit values fail with `LimitError`.
"""
compare(s::Schema, a, b; limits::Limits=Limits()) = comparebytes(s, encode(s, a; limits=limits), encode(s, b; limits=limits); limits=limits)

function checkorderable(s::Schema, visited::Vector{Int32}=Int32[])
    s isa MapSchema && throw(ArgumentError("maps have no sort order (plan §4.12); a map field must be marked order=\"ignore\""))
    s isa ArraySchema && return checkorderable(s.items, visited)
    if s isa UnionSchema
        foreach(b -> checkorderable(b, visited), s.branches)
        return nothing
    end
    if s isa RecordSchema
        id = nodeid(s)
        id in visited && return nothing
        push!(visited, id)
        for f in s.fields
            f.order === :ignore || checkorderable(f.schema, visited)
        end
    end
    return nothing
end

function comparevalue(s::Schema, p::ReadPlan, da::Decoder, db::Decoder)
    countvalues!(da.budget)
    return comparekind(s, p, da, db)
end

"Java's `Double.compare` order: `-0.0 < 0.0`, NaN greater than everything and equal to itself."
function comparefloat(a::AbstractFloat, b::AbstractFloat)
    isnan(a) && return isnan(b) ? 0 : 1
    isnan(b) && return -1
    a < b && return -1
    a > b && return 1
    sa, sb = signbit(a), signbit(b)
    return sa == sb ? 0 : (sa ? -1 : 1)
end

comparekind(::NullSchema, p, da::Decoder, db::Decoder) = 0
comparekind(::BooleanSchema, p, da::Decoder, db::Decoder) = cmp(readbool(da), readbool(db))
comparekind(::IntSchema, p, da::Decoder, db::Decoder) = cmp(readint(da), readint(db))
comparekind(::LongSchema, p, da::Decoder, db::Decoder) = cmp(readlong(da), readlong(db))
comparekind(::FloatSchema, p, da::Decoder, db::Decoder) = comparefloat(readfloat(da), readfloat(db))
comparekind(::DoubleSchema, p, da::Decoder, db::Decoder) = comparefloat(readdouble(da), readdouble(db))
comparekind(s::EnumSchema, p, da::Decoder, db::Decoder) = cmp(readindex(da, length(s.symbols)), readindex(db, length(s.symbols)))

function comparekind(s::Union{BytesSchema,StringSchema}, p, da::Decoder, db::Decoder)
    na = readlen(da, da.budget.limits.max_bytes, :max_bytes)
    nb = readlen(db, db.budget.limits.max_bytes, :max_bytes)
    if s isa StringSchema
        validutf8(da.buf, da.pos, da.pos + na - 1) || dataerror(da, "invalid UTF-8 in string")
        validutf8(db.buf, db.pos, db.pos + nb - 1) || dataerror(db, "invalid UTF-8 in string")
    end
    c = comparebuffers(da.buf, da.pos, na, db.buf, db.pos, nb, da.budget)
    da.pos += na
    db.pos += nb
    return c
end

function comparekind(s::FixedSchema, p, da::Decoder, db::Decoder)
    n = s.size
    n <= remaining(da) || dataerror(da, "fixed of $n bytes exceeds the remaining $(remaining(da)) bytes")
    n <= remaining(db) || dataerror(db, "fixed of $n bytes exceeds the remaining $(remaining(db)) bytes")
    c = comparebuffers(da.buf, da.pos, n, db.buf, db.pos, n, da.budget)
    da.pos += n
    db.pos += n
    return c
end

"Unsigned lexicographic order of two byte ranges (the spec rule for bytes, fixed and UTF-8 strings)."
function comparebuffers(a::AbstractVector{UInt8}, ap::Int, na::Int, b::AbstractVector{UInt8}, bp::Int, nb::Int, budget::Budget)
    n = min(na, nb)
    addcompare!(budget, n)
    for i in 0:n - 1
        x = a[ap + i]
        y = b[bp + i]
        x == y || return x < y ? -1 : 1
    end
    return cmp(na, nb)
end

function comparekind(s::UnionSchema, p::UnionPlan, da::Decoder, db::Decoder)
    n = length(s.branches)
    n == 0 && dataerror(da, "empty union has no datum")
    ia = readindex(da, n)
    ib = readindex(db, n)
    if ia != ib
        skipvalue(p.branches[ia], da)
        skipvalue(p.branches[ib], db)
        return cmp(ia, ib)
    end
    return comparevalue(s.branches[ia], p.branches[ia], da, db)
end

function comparekind(s::RecordSchema, p::RecordPlan, da::Decoder, db::Decoder)
    enter!(da)
    enter!(db)
    result = 0
    for (i, f) in enumerate(s.fields)
        fp = p.fields[i]
        if result != 0 || f.order === :ignore
            skip(fp, da)
            skip(fp, db)
            continue
        end
        c = comparevalue(f.schema, fp, da, db)
        result = f.order === :descending ? -c : c
    end
    leave!(da)
    leave!(db)
    return result
end

"Item-wise iteration over an array's blocks (positive or sized) in lockstep with another decoder."
mutable struct ItemCursor
    remaining::Int
    outerstop::Int
    sized::Bool
    done::Bool
end

ItemCursor() = ItemCursor(0, -1, false, false)

"Position `d` at the next item; `false` once the terminating block has been consumed."
function advance!(c::ItemCursor, d::Decoder, minsize::Int)
    c.done && return false
    while c.remaining == 0
        if c.sized
            d.pos == d.stop + 1 || dataerror(d, "sized array block not exactly consumed")
            d.stop = c.outerstop
            c.sized = false
        end
        count, size = readblockcount(d)
        if count == 0
            c.done = true
            return false
        end
        checkcount(d, count, minsize)
        if size >= 0
            stop = d.pos + size - 1
            stop <= d.stop || dataerror(d, "sized block exceeds the remaining bytes")
            c.outerstop = d.stop
            d.stop = stop
            c.sized = true
        end
        c.remaining = count
    end
    c.remaining -= 1
    return true
end

function comparekind(s::ArraySchema, p::ArrayPlan, da::Decoder, db::Decoder)
    enter!(da)
    enter!(db)
    ca, cb = ItemCursor(), ItemCursor()
    result = 0
    while true
        ha = advance!(ca, da, p.minsize)
        hb = advance!(cb, db, p.minsize)
        (ha || hb) || break
        if !ha
            result == 0 && (result = -1)
            skip(p.items, db)
        elseif !hb
            result == 0 && (result = 1)
            skip(p.items, da)
        elseif result != 0
            skip(p.items, da)
            skip(p.items, db)
        else
            result = comparevalue(s.items, p.items, da, db)
        end
    end
    leave!(da)
    leave!(db)
    return result
end

comparekind(s::MapSchema, p, da::Decoder, db::Decoder) = throw(ArgumentError("maps have no sort order (plan §4.12)"))
