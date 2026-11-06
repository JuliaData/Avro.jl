# Sort order: `comparebytes` orders two encoded datums under a schema without materialising
# them — numerics with Java's `Double.compare` policy (NaN greatest and equal to itself, `-0.0 < 0.0`),
# bytes/fixed/strings unsigned lexicographic, arrays item-wise regardless of block form, enums by
# position, unions by branch then value, records by field order honouring `ascending`/`descending`/
# `ignore`, logical types by their underlying encoding; maps are unorderable outside `ignore` fields.
# `compare` orders Julia values through their encodings, so the two agree for every Avro.jl-produced
# encoding.

"""
    Avro.comparebytes(schema, abytes, bbytes; limits=Limits()) -> Int

`-1`, `0` or `1` ordering two encoded datums under `schema` without decoding them; each
buffer must hold exactly one datum (`DataError` otherwise). Maps are unorderable unless inside an
`order: "ignore"` field (`ArgumentError`).
"""
function comparebytes(s::Schema, a::AbstractVector{UInt8}, b::AbstractVector{UInt8}; limits::Limits=Limits())
    checkorderable(s)
    return comparedatums(s, Decoder(onebased(a), limits), Decoder(onebased(b), limits))
end

"""
    Avro.compare(schema, a, b; limits=Limits()) -> Int

Order two Julia values under `schema` by comparing their encodings (`comparebytes` of
`encode(schema, x)`); cyclic values fail with `LimitError`.
"""
function compare(s::Schema, a, b; limits::Limits=Limits())
    checkorderable(s)
    plan = writeplan(s)
    ea = Encoder(limits)
    eb = Encoder(limits)
    encodedatum!(plan, ea, a, s)
    encodedatum!(plan, eb, b, s)
    decoding = uncapped(limits)
    return comparedatums(s, Decoder(ea.buf, decoding; stop=ea.pos), Decoder(eb.buf, decoding; stop=eb.pos))
end

function comparedatums(s::Schema, da::Decoder, db::Decoder)
    c = comparevalue(s, readplan(s), da, db)
    da.pos == da.stop + 1 || throw(DataError("trailing bytes after the first datum", da.pos))
    db.pos == db.stop + 1 || throw(DataError("trailing bytes after the second datum", db.pos))
    return c
end

function checkorderable(s::Schema, visited::Vector{Bool}=schemaseen(s))
    id = Int(nodeid(s)) + 1
    visited[id] && return nothing
    visited[id] = true
    s isa MapSchema && throw(ArgumentError("maps have no sort order; a map field must be marked order=\"ignore\""))
    s isa ArraySchema && return checkorderable(s.items, visited)
    if s isa UnionSchema
        foreach(b -> checkorderable(b, visited), s.branches)
    elseif s isa RecordSchema
        for f in s.fields
            f.order === :ignore || checkorderable(f.schema, visited)
        end
    end
    return nothing
end

function comparevalue(s::Schema, p::Plan, da::Decoder, db::Decoder)
    countvalue!(da)
    countvalue!(db)
    return comparekind(s, p, da, db)
end

# Java's `Double.compare` order: `-0.0 < 0.0`, NaN greater than everything and equal to itself.
function comparefloat(a::AbstractFloat, b::AbstractFloat)
    isnan(a) && return isnan(b) ? 0 : 1
    isnan(b) && return -1
    a < b && return -1
    a > b && return 1
    sa, sb = signbit(a), signbit(b)
    return sa == sb ? 0 : (sa ? -1 : 1)
end

function comparekind(::NullSchema, p, da::Decoder, db::Decoder)
    return 0
end

function comparekind(::BooleanSchema, p, da::Decoder, db::Decoder)
    return cmp(readbool(da), readbool(db))
end

function comparekind(::IntSchema, p, da::Decoder, db::Decoder)
    return cmp(readint(da), readint(db))
end

function comparekind(::LongSchema, p, da::Decoder, db::Decoder)
    return cmp(readlong(da), readlong(db))
end

function comparekind(::FloatSchema, p, da::Decoder, db::Decoder)
    return comparefloat(readfloat(da), readfloat(db))
end

function comparekind(::DoubleSchema, p, da::Decoder, db::Decoder)
    return comparefloat(readdouble(da), readdouble(db))
end

function comparekind(s::EnumSchema, p, da::Decoder, db::Decoder)
    return cmp(readindex(da, length(s.symbols)), readindex(db, length(s.symbols)))
end

function comparekind(s::Union{BytesSchema,StringSchema}, p, da::Decoder, db::Decoder)
    na = readlen(da)
    nb = readlen(db)
    if s isa StringSchema
        validutf8(da.buf, da.pos, da.pos + na - 1) || dataerror(da, "invalid UTF-8 in string")
        validutf8(db.buf, db.pos, db.pos + nb - 1) || dataerror(db, "invalid UTF-8 in string")
    end
    c = comparebuffers(da.buf, da.pos, na, db.buf, db.pos, nb)
    da.pos += na
    db.pos += nb
    return c
end

function comparekind(s::FixedSchema, p, da::Decoder, db::Decoder)
    n = s.size
    n <= remaining(da) || dataerror(da, "fixed of $n bytes exceeds the remaining $(remaining(da)) bytes")
    n <= remaining(db) || dataerror(db, "fixed of $n bytes exceeds the remaining $(remaining(db)) bytes")
    c = comparebuffers(da.buf, da.pos, n, db.buf, db.pos, n)
    da.pos += n
    db.pos += n
    return c
end

# Unsigned lexicographic order of two byte ranges (the spec rule for bytes, fixed and UTF-8 strings).
function comparebuffers(a::AbstractVector{UInt8}, ap::Int, na::Int, b::AbstractVector{UInt8}, bp::Int, nb::Int)
    n = min(na, nb)
    for i in 0:n - 1
        x = a[ap + i]
        y = b[bp + i]
        x != y && return x < y ? -1 : 1
    end
    return cmp(na, nb)
end

function comparekind(s::UnionSchema, p::UnionPlan, da::Decoder, db::Decoder)
    n = length(s.branches)
    n == 0 && dataerror(da, "empty union has no datum")
    ia = readindex(da, n)
    ib = readindex(db, n)
    if ia != ib
        skip(p.branches[ia], da)
        skip(p.branches[ib], db)
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

# Item-wise iteration over an array's blocks (positive or sized) in lockstep with another decoder.
mutable struct ItemCursor
    remaining::Int
    outerstop::Int
    sized::Bool
    done::Bool
end

function ItemCursor()
    return ItemCursor(0, -1, false, false)
end

# Position `d` at the next item; `false` once the terminating block has been consumed.
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
            stop = checked_add(d.pos, size) - 1
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

function comparekind(s::MapSchema, p, da::Decoder, db::Decoder)
    throw(ArgumentError("maps have no sort order"))
end
