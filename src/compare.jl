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
    return withbudget(limits) do budget
        checkorderable(s, budget)
        asource = positionalsource(a, 1, limits, budget)
        bsource = positionalsource(b, 1, limits, budget)
        spans = spanplan(s; budget=budget)
        inputmax = satadd(budget.input_bytes,
                          satadd(datumspanmax(asource.buffer, asource.start, limits),
                                 datumspanmax(bsource.buffer, bsource.start, limits)))
        da = Decoder(asource.buffer, budget; pos=asource.start,
                     validate=validate)
        db = Decoder(bsource.buffer, budget; pos=bsource.start,
                     validate=validate)
        datumspan!(spans, da, asource.start, budget.values, inputmax)
        aspan = decoderspan(da)
        datumspan!(spans, db, bsource.start,
                   checkedvalueadd(budget, budget.values, aspan.values),
                   inputmax)
        bspan = decoderspan(db)
        anext = originalnext(asource, aspan.next)
        bnext = originalnext(bsource, bspan.next)
        anext == asource.sourceend + 1 || throw(DataError("trailing bytes after the first datum", anext))
        bnext == bsource.sourceend + 1 || throw(DataError("trailing bytes after the second datum", bnext))
        input = checked_add(aspan.bytes, bspan.bytes)
        addinput!(budget, input)
        plan = readplan(s; budget=budget)
        da.stop = aspan.next - 1
        db.stop = bspan.next - 1
        admittedresult = 0
        validate === :fast &&
            ((aspan, bspan, admittedresult) =
                 admitcomparespans(s, plan, da, db, aspan, bspan))
        values = checkedvalueadd(budget, aspan.values, bspan.values)
        checkworkscope!(budget, aspan.values, aspan.bytes)
        checkworkscope!(budget, bspan.values, bspan.bytes)
        checkworkscope!(budget, values, input; projected=true)
        if validate === :fast
            countvalues!(budget, values)
            checkoperationwork!(budget)
            return admittedresult
        end
        values0 = budget.values
        c = comparevalue(s, plan, da, db)
        actual = budget.values - values0
        actual < values && countvalues!(budget, values - actual)
        da.pos == aspan.next || throw(DataError("first datum did not consume its proven span", da.pos))
        db.pos == bspan.next || throw(DataError("second datum did not consume its proven span", db.pos))
        checkoperationwork!(budget)
        return c
    end
end

"""
    Avro.compare(schema, a, b; limits=Limits()) -> Int

Order two Julia values under `schema` by comparing their canonical encodings (`comparebytes` of
`encode(schema, x)`); cyclic or over-limit values fail with `LimitError`.
"""
function compare(s::Schema, a, b; limits::Limits=Limits())
    return withbudget(limits; direction=:encode) do budget
        local result
        beginworkdefer!(budget)
        try
            checkorderable(s, budget)
            writer = writeplan(s; budget=budget)
            ea = Encoder(budget)
            eb = Encoder(budget)
            encodedatum!(writer, ea, a, s)
            encodedatum!(writer, eb, b, s)
            spans = spanplan(s; budget=budget)
            da = Decoder(ea.buf, budget; stop=ea.pos)
            db = Decoder(eb.buf, budget; stop=eb.pos)
            encodedinput = satadd(ea.pos, eb.pos)
            datumspan!(spans, da, 1, 0, encodedinput)
            aspan = decoderspan(da)
            datumspan!(spans, db, 1, aspan.values, encodedinput)
            bspan = decoderspan(db)
            aspan.next == ea.pos + 1 || throw(DataError("internal error: first encoding has a trailing suffix", aspan.next))
            bspan.next == eb.pos + 1 || throw(DataError("internal error: second encoding has a trailing suffix", bspan.next))
            values = checkedvalueadd(budget, aspan.values, bspan.values)
            checkworkscope!(budget, aspan.values, aspan.bytes)
            checkworkscope!(budget, bspan.values, bspan.bytes)
            checkworkscope!(budget, values,
                            checked_add(aspan.bytes, bspan.bytes);
                            projected=true)
            reader = readplan(s; budget=budget)
            da.stop = aspan.next - 1
            db.stop = bspan.next - 1
            values0 = budget.values
            result = comparevalue(s, reader, da, db)
            actual = budget.values - values0
            actual < values && countvalues!(budget, values - actual)
            da.pos == ea.pos + 1 || throw(DataError("trailing bytes after the first datum", da.pos))
            db.pos == eb.pos + 1 || throw(DataError("trailing bytes after the second datum", db.pos))
        finally
            endworkdefer!(budget)
        end
        checkoperationwork!(budget)
        checkcomparisonwork!(budget)
        return result
    end
end

function checkorderable(s::Schema, budget::Budget)
    n = graphinfo(s).nodes
    charge = vectorbytes(Bool, n)
    reserve!(budget, charge)
    visited = fill(false, n)
    allocated!(budget, charge)
    try
        return checkorderable(s, visited, budget)
    finally
        release!(budget, charge)
    end
end

function checkorderable(s::Schema, visited::Vector{Bool}, budget::Budget)
    id = Int(nodeid(s)) + 1
    visited[id] && return nothing
    visited[id] = true
    addresolution!(budget)
    s isa MapSchema && throw(ArgumentError("maps have no sort order (plan §4.12); a map field must be marked order=\"ignore\""))
    s isa ArraySchema && return checkorderable(s.items, visited, budget)
    if s isa UnionSchema
        foreach(b -> checkorderable(b, visited, budget), s.branches)
        return nothing
    end
    if s isa RecordSchema
        for f in s.fields
            f.order === :ignore || checkorderable(f.schema, visited, budget)
        end
    end
    return nothing
end

function comparevalue(s::Schema, p::ReadPlan, da::Decoder, db::Decoder)
    countvalues!(da.budget, 2)
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

function admitcomparevalue!(schema::Schema, plan::ReadPlan,
                            da::Decoder, db::Decoder,
                            ca::SyntheticCounter, cb::SyntheticCounter)
    countwriter!(ca)
    countwriter!(cb)
    return admitcomparekind!(schema, plan, da, db, ca, cb)
end

function admitcomparekind!(::NullSchema, plan, da, db, ca, cb)
    return 0
end

function admitcomparekind!(::BooleanSchema, plan, da, db, ca, cb)
    return cmp(readbool(da), readbool(db))
end

function admitcomparekind!(::IntSchema, plan, da, db, ca, cb)
    return cmp(readint(da), readint(db))
end

function admitcomparekind!(::LongSchema, plan, da, db, ca, cb)
    return cmp(readlong(da), readlong(db))
end

function admitcomparekind!(::FloatSchema, plan, da, db, ca, cb)
    return comparefloat(readfloat(da), readfloat(db))
end

function admitcomparekind!(::DoubleSchema, plan, da, db, ca, cb)
    return comparefloat(readdouble(da), readdouble(db))
end

function admitcomparekind!(schema::EnumSchema, plan, da, db, ca, cb)
    return cmp(readindex(da, length(schema.symbols)),
               readindex(db, length(schema.symbols)))
end

function admitcomparekind!(schema::Union{BytesSchema,StringSchema}, plan,
                           da, db, ca, cb)
    na = readlen(da, da.budget.limits.max_bytes, :max_bytes)
    nb = readlen(db, db.budget.limits.max_bytes, :max_bytes)
    if schema isa StringSchema
        validutf8(da.buf, da.pos, da.pos + na - 1) ||
            dataerror(da, "invalid UTF-8 in string")
        validutf8(db.buf, db.pos, db.pos + nb - 1) ||
            dataerror(db, "invalid UTF-8 in string")
    end
    result = comparebuffers(da.buf, da.pos, na, db.buf, db.pos, nb,
                            da.budget)
    da.pos += na
    db.pos += nb
    return result
end

function admitcomparekind!(schema::FixedSchema, plan, da, db, ca, cb)
    count = schema.size
    checkvaluebytes(da.budget, count)
    count <= remaining(da) ||
        dataerror(da, "fixed of $count bytes exceeds the remaining $(remaining(da)) bytes")
    count <= remaining(db) ||
        dataerror(db, "fixed of $count bytes exceeds the remaining $(remaining(db)) bytes")
    result = comparebuffers(da.buf, da.pos, count, db.buf, db.pos, count,
                            da.budget)
    da.pos += count
    db.pos += count
    return result
end

function admitcomparekind!(schema::UnionSchema, plan::UnionPlan,
                           da, db, ca, cb)
    count = length(schema.branches)
    count == 0 && dataerror(da, "empty union has no datum")
    ia = readindex(da, count)
    ib = readindex(db, count)
    if ia != ib
        syntheticvalue!(plan.branches[ia], da, false, ca)
        syntheticvalue!(plan.branches[ib], db, false, cb)
        return cmp(ia, ib)
    end
    return admitcomparevalue!(schema.branches[ia], plan.branches[ia],
                              da, db, ca, cb)
end

function admitcomparekind!(schema::RecordSchema, plan::RecordPlan,
                           da, db, ca, cb)
    enter!(da)
    enter!(db)
    result = 0
    try
        for (index, field) in enumerate(schema.fields)
            fieldplan = plan.fields[index]
            if result != 0 || field.order === :ignore
                syntheticvalue!(fieldplan, da, false, ca)
                syntheticvalue!(fieldplan, db, false, cb)
                continue
            end
            compared = admitcomparevalue!(field.schema, fieldplan,
                                          da, db, ca, cb)
            result = field.order === :descending ? -compared : compared
        end
    finally
        leave!(da)
        leave!(db)
    end
    return result
end

function restoreitemcursor!(cursor, decoder::Decoder)
    cursor.sized && (decoder.stop = cursor.outerstop)
    return nothing
end

function admitcomparekind!(schema::ArraySchema, plan::ArrayPlan,
                           da, db, ca, cb)
    enter!(da)
    enter!(db)
    cursora = ItemCursor()
    cursorb = ItemCursor()
    result = 0
    try
        while true
            hasa = advance!(cursora, da, plan.minsize)
            hasb = advance!(cursorb, db, plan.minsize)
            (hasa || hasb) || break
            if !hasa
                result == 0 && (result = -1)
                syntheticvalue!(plan.items, db, false, cb)
            elseif !hasb
                result == 0 && (result = 1)
                syntheticvalue!(plan.items, da, false, ca)
            elseif result != 0
                syntheticvalue!(plan.items, da, false, ca)
                syntheticvalue!(plan.items, db, false, cb)
            else
                result = admitcomparevalue!(schema.items, plan.items,
                                            da, db, ca, cb)
            end
        end
    finally
        restoreitemcursor!(cursora, da)
        restoreitemcursor!(cursorb, db)
        leave!(da)
        leave!(db)
    end
    return result
end

function admitcomparekind!(::MapSchema, plan, da, db, ca, cb)
    throw(ArgumentError("maps have no sort order (plan §4.12)"))
end

"Run the data-aware fast consumer pass for both comparison inputs."
function admitcomparespans(schema::Schema, plan::ReadPlan,
                           da::Decoder, db::Decoder,
                           aspan::DatumSpan, bspan::DatumSpan)
    pa = Decoder(da.buf, da.budget; pos=da.pos, stop=aspan.next - 1,
                 validate=:fast)
    pb = Decoder(db.buf, db.budget; pos=db.pos, stop=bspan.next - 1,
                 validate=:fast)
    base = da.budget.values
    pa.scopevalues = 0
    scope = SyntheticScope(pa)
    ca = SyntheticCounter(da.budget, aspan.bytes, base, 0, true, pa, scope, 0)
    cb = SyntheticCounter(db.budget, bspan.bytes, base, 0, true, pb, scope, 0)
    countsynthetic!(ca, 0)
    countsynthetic!(cb, 0)
    result = admitcomparevalue!(schema, plan, pa, pb, ca, cb)
    pa.pos == aspan.next ||
        throw(DataError("comparison scan did not consume the first datum", pa.pos))
    pb.pos == bspan.next ||
        throw(DataError("comparison scan did not consume the second datum", pb.pos))
    values = checkedvalueadd(da.budget, syntheticvalues(ca),
                             syntheticvalues(cb))
    values == syntheticvalues(scope) ||
        throw(DataError("internal error: comparison value count mismatch", pa.pos))
    return (DatumSpan(aspan.next, aspan.bytes, syntheticvalues(ca), false),
            DatumSpan(bspan.next, bspan.bytes, syntheticvalues(cb), false), result)
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
    for i in 0:n - 1
        x = a[ap + i]
        y = b[bp + i]
        if x != y
            addcompare!(budget, i + 1)
            return x < y ? -1 : 1
        end
    end
    addcompare!(budget, n)
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

"Item-wise iteration over an array's blocks (positive or sized) in lockstep with another decoder."
mutable struct ItemCursor
    remaining::Int
    outerstop::Int
    sized::Bool
    done::Bool
end

function ItemCursor()
    return ItemCursor(0, -1, false, false)
end

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
    throw(ArgumentError("maps have no sort order (plan §4.12)"))
end
