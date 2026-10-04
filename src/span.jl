# Allocation-free encoded-datum span plans. A span pass finds the exact end and structural value count
# of one writer datum before its bytes are admitted to the work rule. It never materialises values and
# collapses fixed-layout subtrees, including zero-width records and array items.

abstract type SpanPlan end

"The retained box of one span-plan node held behind the abstract `SpanPlan` interface."
function spannodebytes(::Type{P}) where {P<:SpanPlan}
    return isbitstype(P) ? boxbytes(P) : 16 + sizeof(P)
end

struct FixedSpan <: SpanPlan
    width::Int
    values::Int
end

"A fixed datum whose width is also bounded by `max_bytes`."
struct FixedValueSpan <: SpanPlan
    width::Int
    values::Int
end

struct IntSpan <: SpanPlan end
struct LongSpan <: SpanPlan end

struct BytesSpan <: SpanPlan end

struct EnumSpan <: SpanPlan
    symbols::Int
end

struct ArraySpan <: SpanPlan
    items::SpanPlan
    itemwidth::Int
    itemvalues::Int
    minsize::Int
end

struct MapSpan <: SpanPlan
    values::SpanPlan
    valuevalues::Int
    minsize::Int
end

struct UnionSpan <: SpanPlan
    branches::Vector{SpanPlan}
end

struct SpanStep
    width::Int
    values::Int
    maxfixed::Int
    plan::Union{Nothing,SpanPlan}
end

mutable struct RecordSpan <: SpanPlan
    const fields::Vector{SpanPlan}
    const steps::Vector{SpanStep}
    nsteps::Int
    basevalues::Int
    width::Int
    values::Int
    maxfixed::Int
    finite::Bool
end

function spanwidth(p::SpanPlan)
    return p isa Union{FixedSpan,FixedValueSpan} ? p.width : p isa RecordSpan ? p.width : -1
end

function spanvalues(p::SpanPlan)
    p isa Union{FixedSpan,FixedValueSpan} && return p.values
    p isa Union{IntSpan,LongSpan,BytesSpan,EnumSpan} && return 1
    p isa RecordSpan && return p.values
    return -1
end

"The largest fixed datum hidden inside one fixed-width span, or zero when there is none."
function spanmaxfixed(::SpanPlan)
    return 0
end

function spanmaxfixed(p::FixedValueSpan)
    return p.width
end

function spanmaxfixed(p::RecordSpan)
    return p.maxfixed
end

function spanfinite(::Union{FixedSpan,FixedValueSpan,IntSpan,LongSpan,BytesSpan})
    return true
end

function spanfinite(plan::EnumSpan)
    return plan.symbols > 0
end

function spanfinite(::Union{ArraySpan,MapSpan})
    return true
end

function spanfinite(plan::UnionSpan)
    return any(spanfinite, plan.branches)
end

function spanfinite(plan::RecordSpan)
    return plan.finite
end

"Compute the least fixed point for records that have at least one finite datum."
function settlespanfinite!(memo::Vector{Union{Nothing,SpanPlan}}, budget)
    changed = true
    while changed
        changed = false
        for candidate in memo
            candidate isa RecordSpan || continue
            budget === nothing || addresolution!(budget)
            candidate.finite && continue
            finite = true
            for field in candidate.fields
                budget === nothing || addresolution!(budget)
                if !spanfinite(field)
                    finite = false
                    break
                end
            end
            if finite
                candidate.finite = true
                changed = true
            end
        end
    end
    return nothing
end

"Build the allocation-free span plan of a writer schema, memoised by its dense node ids."
function spanplan(s::Schema; budget::Union{Nothing,Budget}=nothing)
    nodes = graphinfo(s).nodes
    memocharge = vectorbytes(Union{Nothing,SpanPlan}, nodes)
    checkpoint = budget === nothing ? nothing : budgetcheckpoint(budget)
    try
        budget === nothing || reserve!(budget, memocharge)
        memo = Vector{Union{Nothing,SpanPlan}}(nothing, nodes)
        budget === nothing || allocated!(budget, memocharge)
        plan = spanplan(s, memo, budget)
        settlespanfinite!(memo, budget)
        budget === nothing || release!(budget, memocharge)
        return plan
    catch
        budget === nothing || rollbackreservations!(budget, checkpoint::NTuple{2,Int})
        rethrow()
    end
end

function spanplan(s::Schema, memo::Vector{Union{Nothing,SpanPlan}}, budget)
    id = Int(nodeid(s)) + 1
    plan = memo[id]
    plan === nothing || return plan
    budget === nothing || addresolution!(budget)
    plan = buildspanplan(s, memo, budget)
    memo[id] = plan
    return plan
end

function buildspanplan(::NullSchema, memo, budget)
    return plannode(() -> FixedSpan(0, 1), budget, spannodebytes(FixedSpan))
end

function buildspanplan(::BooleanSchema, memo, budget)
    return plannode(() -> FixedSpan(1, 1), budget, spannodebytes(FixedSpan))
end

function buildspanplan(::IntSchema, memo, budget)
    return IntSpan()
end

function buildspanplan(s::EnumSchema, memo, budget)
    return plannode(() -> EnumSpan(length(s.symbols)), budget, spannodebytes(EnumSpan))
end

function buildspanplan(::LongSchema, memo, budget)
    return LongSpan()
end

function buildspanplan(::FloatSchema, memo, budget)
    return plannode(() -> FixedSpan(4, 1), budget, spannodebytes(FixedSpan))
end

function buildspanplan(::DoubleSchema, memo, budget)
    return plannode(() -> FixedSpan(8, 1), budget, spannodebytes(FixedSpan))
end

function buildspanplan(::Union{BytesSchema,StringSchema}, memo, budget)
    return BytesSpan()
end

function buildspanplan(s::FixedSchema, memo, budget)
    return plannode(() -> FixedValueSpan(s.size, 1), budget,
                    spannodebytes(FixedValueSpan))
end

function buildspanplan(s::ArraySchema, memo, budget)
    items = spanplan(s.items, memo, budget)
    ms = budget === nothing ? minsize(s.items) : minsize(s.items, budget)
    return plannode(() -> ArraySpan(items, spanwidth(items), spanvalues(items), ms), budget,
                    spannodebytes(ArraySpan))
end

function buildspanplan(s::MapSchema, memo, budget)
    values = spanplan(s.values, memo, budget)
    ms = budget === nothing ? minsize(s.values) : minsize(s.values, budget)
    return plannode(() -> MapSpan(values, spanvalues(values), ms), budget,
                    spannodebytes(MapSpan))
end

function buildspanplan(s::UnionSchema, memo, budget)
    n = length(s.branches)
    charge = checked_add(vectorbytes(SpanPlan, n), spannodebytes(UnionSpan))
    budget === nothing || reserve!(budget, charge)
    branches = Vector{SpanPlan}(undef, n)
    budget === nothing || allocated!(budget, vectorbytes(SpanPlan, n))
    for (i, branch) in enumerate(s.branches)
        branches[i] = spanplan(branch, memo, budget)
    end
    plan = UnionSpan(branches)
    budget === nothing || allocated!(budget, spannodebytes(UnionSpan))
    return plan
end

function buildspanplan(s::RecordSchema, memo, budget)
    n = length(s.fields)
    charge = checked_add(checked_add(vectorbytes(SpanPlan, n),
                                     vectorbytes(SpanStep, n)),
                         spannodebytes(RecordSpan))
    budget === nothing || reserve!(budget, charge)
    fields = Vector{SpanPlan}(undef, n)
    steps = Vector{SpanStep}(undef, n)
    plan = RecordSpan(fields, steps, 0, 1, -1, -1, 0, false)
    budget === nothing || allocated!(budget, charge)
    memo[Int(nodeid(s)) + 1] = plan
    width = 0
    values = 1
    fixedwidth = true
    fixedvalues = true
    basevalues = 1
    pendingwidth = 0
    pendingvalues = 0
    pendingmaxfixed = 0
    maxfixed = 0
    nsteps = 0
    for (index, field) in enumerate(s.fields)
        child = spanplan(field.schema, memo, budget)
        fields[index] = child
        childwidth = spanwidth(child)
        childvalues = spanvalues(child)
        childmaxfixed = spanmaxfixed(child)
        maxfixed = max(maxfixed, childmaxfixed)
        if fixedwidth && childwidth >= 0
            width = satadd(width, childwidth)
        else
            fixedwidth = false
        end
        if fixedvalues && childvalues >= 0
            values = satadd(values, childvalues)
        else
            fixedvalues = false
        end
        if childwidth >= 0
            if childwidth == 0
                basevalues = satadd(basevalues, childvalues)
            else
                pendingwidth = satadd(pendingwidth, childwidth)
                pendingvalues = satadd(pendingvalues, childvalues)
                pendingmaxfixed = max(pendingmaxfixed, childmaxfixed)
            end
        else
            if pendingwidth > 0
                nsteps += 1
                steps[nsteps] = SpanStep(pendingwidth, pendingvalues,
                                         pendingmaxfixed, nothing)
                pendingwidth = 0
                pendingvalues = 0
                pendingmaxfixed = 0
            end
            nsteps += 1
            steps[nsteps] = SpanStep(-1, 0, 0, child)
        end
    end
    if pendingwidth > 0
        nsteps += 1
        steps[nsteps] = SpanStep(pendingwidth, pendingvalues,
                                 pendingmaxfixed, nothing)
    end
    plan.nsteps = nsteps
    plan.basevalues = basevalues
    plan.width = fixedwidth ? width : -1
    plan.values = fixedvalues ? values : -1
    plan.maxfixed = maxfixed
    return plan
end

mutable struct SpanCursor{B<:AbstractVector{UInt8}}
    const buf::B
    pos::Int
    const stop::Int
    const start::Int
    const limits::Limits
    const datummax::Int
    const values0::Int
    const inputmax::Int
    const validate::Symbol
    values::Int
    depth::Int
    blind::Bool
end

const SpanState = Union{SpanCursor,Decoder}

function spanerror(cursor::SpanState, message::AbstractString, position::Int=cursor.pos)
    throw(DataError(message, position))
end

function spanconsumed(cursor::SpanState, next::Int=cursor.pos)
    return next - cursor.start
end

function checkspanlimit(cursor::SpanState, next::Int)
    consumed = spanconsumed(cursor, next)
    consumed <= cursor.limits.max_datum_bytes ||
        throw(LimitError(:max_datum_bytes, consumed, cursor.limits.max_datum_bytes,
                         :max_datum_bytes, :decode))
    return nothing
end

function spanadvance!(cursor::SpanState, count::Int)
    count >= 0 || spanerror(cursor, "negative span advance $count")
    consumed = spanconsumed(cursor)
    observed = satadd(consumed, count)
    observed <= cursor.limits.max_datum_bytes ||
        throw(LimitError(:max_datum_bytes, observed,
                         cursor.limits.max_datum_bytes,
                         :max_datum_bytes, :decode))
    count <= cursor.stop - cursor.pos + 1 ||
        spanerror(cursor, "value exceeds the remaining $(cursor.stop - cursor.pos + 1) bytes")
    count <= typemax(Int) - cursor.pos ||
        spanerror(cursor, "value end position exceeds the Int range")
    cursor.pos += count
    return nothing
end

@inline function spanbyte!(cursor::SpanState)
    next = checked_add(cursor.pos, 1)
    checkspanlimit(cursor, next)
    cursor.pos <= cursor.stop || spanerror(cursor, "unexpected end of data")
    @inbounds byte = cursor.buf[cursor.pos]
    cursor.pos = next
    return byte
end

function spanlong!(cursor::SpanState)
    start = cursor.pos
    byte = spanbyte!(cursor)
    value = UInt64(byte & 0x7f)
    shift = 7
    while byte >= 0x80
        shift > 63 && spanerror(cursor, "varint longer than 10 bytes", start)
        byte = spanbyte!(cursor)
        if shift == 63
            (byte & 0x7e) == 0 || spanerror(cursor, "varint overflows 64 bits", start)
            value |= UInt64(byte & 0x01) << 63
        else
            value |= UInt64(byte & 0x7f) << shift
        end
        shift += 7
    end
    return reinterpret(Int64, (value >> 1) ⊻ (-(value & UInt64(1))))
end

function spanint!(cursor::SpanState)
    start = cursor.pos
    byte = spanbyte!(cursor)
    value = UInt32(byte & 0x7f)
    shift = 7
    while byte >= 0x80
        shift > 28 && spanerror(cursor, "int varint longer than 5 bytes", start)
        byte = spanbyte!(cursor)
        if shift == 28
            (byte & 0x70) == 0 || spanerror(cursor, "int varint overflows 32 bits", start)
            value |= UInt32(byte & 0x0f) << 28
        else
            value |= UInt32(byte & 0x7f) << shift
        end
        shift += 7
    end
    decoded = reinterpret(Int32, (value >> 1) ⊻ (-(value & UInt32(1))))
    return decoded
end

function spanlength!(cursor::SpanState, maximum::Int, limit::Symbol)
    length = spanlong!(cursor)
    length >= 0 || spanerror(cursor, "negative length $length")
    length <= maximum || throw(LimitError(limit, Int(length), maximum, limit, :decode))
    length <= typemax(Int) || throw(LimitError(limit, typemax(Int), maximum, limit, :decode))
    return Int(length)
end

function spanblockcount!(cursor::SpanState)
    count = spanlong!(cursor)
    if count >= 0
        count <= cursor.limits.max_block_count ||
            throw(LimitError(:max_block_count, Int(count), cursor.limits.max_block_count,
                             :max_block_count, :decode))
        return (Int(count), -1)
    end
    count == typemin(Int64) && spanerror(cursor, "block count typemin(Int64)")
    count = -count
    count <= cursor.limits.max_block_count ||
        throw(LimitError(:max_block_count, Int(count), cursor.limits.max_block_count,
                         :max_block_count, :decode))
    size = spanlength!(cursor, cursor.limits.max_datum_bytes, :max_datum_bytes)
    return (Int(count), size)
end

"Reject a declared collection count that cannot fit any datum in the remaining bytes."
function checkspancount(cursor::SpanState, count::Int, minsize::Int, finite::Bool)
    minsize <= 0 && return nothing
    !finite && count > 0 &&
        spanerror(cursor, "block declares $count items of a schema with no finite datum")
    minsize == INFINITE && return nothing
    remaining = cursor.stop - cursor.pos + 1
    count <= remaining ÷ minsize ||
        spanerror(cursor, "block declares $count items but only $remaining bytes remain")
    return nothing
end

function enterspan!(cursor::SpanState)
    cursor.depth += 1
    cursor.depth <= cursor.limits.max_depth ||
        throw(LimitError(:max_depth, cursor.depth, cursor.limits.max_depth, :max_depth, :decode))
    return nothing
end

function leavespan!(cursor::SpanState)
    cursor.depth -= 1
    return nothing
end

function checkspanvalues(cursor::SpanState, values::Int)
    values <= cursor.limits.max_total_values ||
        throw(LimitError(:max_total_values, values,
                         cursor.limits.max_total_values,
                         :max_total_values, :decode))
    return values
end

function checkblockspanvalues!(::SpanCursor, values::Int)
    return nothing
end

"Check projected values against the active block's independent work scope."
function checkblockspanvalues!(decoder::Decoder, values::Int,
                               basevalues::Int=decoder.values0)
    decoder.blockvalues0 < 0 && return nothing
    prior = basevalues - decoder.blockvalues0
    prior >= 0 || throw(ArgumentError("block value base precedes the active block"))
    observed = spanvalueadd(decoder, prior, values)
    input = decoder.budget.input_bytes - decoder.blockinput0
    input >= 0 || throw(ArgumentError("block input base exceeds operation input"))
    cap = muladdcap(decoder.limits.max_values_per_byte, input,
                    decoder.limits.work_allowance)
    observed <= cap ||
        throw(limiterror(decoder.budget, :max_values_per_byte, observed, cap))
    return nothing
end

"Count exact span work and reject as soon as no possible remaining input can make it legal."
function countspanvalues!(cursor::SpanState, count::Int)
    count >= 0 || throw(ArgumentError("span value count must be non-negative"))
    limits = cursor.limits
    values = spanvalueadd(cursor, cursor.values, count)
    datumcap = muladdcap(limits.max_values_per_byte, cursor.datummax,
                        limits.work_allowance)
    values <= datumcap ||
        throw(LimitError(:max_values_per_byte, values, datumcap,
                         :max_values_per_byte, :decode))
    checkblockspanvalues!(cursor, values)
    total = spanvalueadd(cursor, cursor.values0, values)
    operationcap = muladdcap(limits.max_values_per_byte, cursor.inputmax,
                             limits.work_allowance)
    total <= operationcap ||
        throw(LimitError(:max_values_per_byte, total, operationcap,
                         :max_values_per_byte, :decode))
    cursor.values = values
    return count
end

"Add observed span values and report a closed limit error instead of leaking overflow."
function spanvalueadd(cursor::SpanState, a::Int, b::Int)
    a >= 0 && b >= 0 || throw(ArgumentError("span values must be non-negative"))
    maximum = cursor.limits.max_total_values
    b <= typemax(Int) - a ||
        throw(LimitError(:max_total_values, typemax(Int), maximum,
                         :max_total_values, :decode))
    values = a + b
    values <= maximum ||
        throw(LimitError(:max_total_values, values, maximum,
                         :max_total_values, :decode))
    return values
end

function checkspanfixed(cursor::SpanState, width::Int)
    width <= cursor.limits.max_bytes ||
        throw(LimitError(:max_bytes, width, cursor.limits.max_bytes,
                         :max_bytes, :decode))
    return nothing
end

"Multiply attacker-controlled span counts without leaking a raw overflow."
function spanproduct(cursor::SpanState, count::Int, width::Int,
                     limit::Symbol, maximum::Int)
    count == 0 && return 0
    width <= div(maximum, count) ||
        throw(LimitError(limit, typemax(Int), maximum, limit, :decode))
    return count * width
end

function spanvalue!(plan::FixedSpan, cursor::SpanState)
    countspanvalues!(cursor, plan.values)
    spanadvance!(cursor, plan.width)
    return checkspanvalues(cursor, plan.values)
end

function spanvalue!(plan::FixedValueSpan, cursor::SpanState)
    countspanvalues!(cursor, plan.values)
    checkspanfixed(cursor, plan.width)
    spanadvance!(cursor, plan.width)
    return checkspanvalues(cursor, plan.values)
end

function spanvalue!(::IntSpan, cursor::SpanState)
    countspanvalues!(cursor, 1)
    spanint!(cursor)
    return checkspanvalues(cursor, 1)
end

function spanvalue!(::LongSpan, cursor::SpanState)
    countspanvalues!(cursor, 1)
    spanlong!(cursor)
    return checkspanvalues(cursor, 1)
end

function spanvalue!(::BytesSpan, cursor::SpanState)
    countspanvalues!(cursor, 1)
    length = spanlength!(cursor, cursor.limits.max_bytes, :max_bytes)
    spanadvance!(cursor, length)
    return checkspanvalues(cursor, 1)
end

function spanvalue!(plan::EnumSpan, cursor::SpanState)
    countspanvalues!(cursor, 1)
    index = spanlong!(cursor)
    0 <= index < plan.symbols || spanerror(cursor, "index $index out of range (0:$(plan.symbols - 1))")
    return checkspanvalues(cursor, 1)
end

function spanarrayitems!(plan::ArraySpan, cursor::SpanState, count::Int)
    if plan.itemvalues >= 0
        values = checkspanvalues(cursor,
            spanproduct(cursor, count, plan.itemvalues, :max_total_values,
                        cursor.limits.max_total_values))
        if plan.itemwidth >= 0
            countspanvalues!(cursor, values)
            checkspanfixed(cursor, spanmaxfixed(plan.items))
            spanadvance!(cursor,
                spanproduct(cursor, count, plan.itemwidth, :max_datum_bytes,
                            cursor.limits.max_datum_bytes))
        else
            for _ in 1:count
                spanvalue!(plan.items, cursor)
            end
        end
        return values
    end
    values = 0
    for _ in 1:count
        values = spanvalueadd(cursor, values, spanvalue!(plan.items, cursor))
        checkspanvalues(cursor, values)
    end
    return values
end

function spanvalue!(plan::ArraySpan, cursor::SpanState)
    enterspan!(cursor)
    values = 1
    countspanvalues!(cursor, values)
    checkspanvalues(cursor, values)
    try
        while true
            count, size = spanblockcount!(cursor)
            count == 0 && break
            checkspancount(cursor, count, plan.minsize, spanfinite(plan.items))
            blockend = size < 0 ? -1 : checked_add(cursor.pos, size)
            size < 0 || (checkspanlimit(cursor, blockend); blockend - 1 <= cursor.stop ||
                         spanerror(cursor, "sized array block exceeds the remaining bytes"))
            if size >= 0 && cursor.validate === :fast
                checkspanfixed(cursor, spanmaxfixed(plan.items))
                cursor.blind = true
                cursor.pos = blockend
                continue
            end
            values = spanvalueadd(cursor, values,
                                  spanarrayitems!(plan, cursor, count))
            checkspanvalues(cursor, values)
            size >= 0 && cursor.pos != blockend &&
                spanerror(cursor, "sized array block not exactly consumed")
        end
    finally
        leavespan!(cursor)
    end
    return values
end

function spanvalue!(plan::MapSpan, cursor::SpanState)
    enterspan!(cursor)
    values = 1
    countspanvalues!(cursor, values)
    checkspanvalues(cursor, values)
    try
        while true
            count, size = spanblockcount!(cursor)
            count == 0 && break
            checkspancount(cursor, count, satadd(plan.minsize, 1),
                           spanfinite(plan.values))
            blockend = size < 0 ? -1 : checked_add(cursor.pos, size)
            size < 0 || (checkspanlimit(cursor, blockend); blockend - 1 <= cursor.stop ||
                         spanerror(cursor, "sized map block exceeds the remaining bytes"))
            if size >= 0 && cursor.validate === :fast
                checkspanfixed(cursor, spanmaxfixed(plan.values))
                cursor.blind = true
                cursor.pos = blockend
                continue
            end
            if plan.valuevalues >= 0
                values = spanvalueadd(cursor, values,
                                      spanproduct(cursor, count,
                                                  plan.valuevalues,
                                                  :max_total_values,
                                                  cursor.limits.max_total_values))
                checkspanvalues(cursor, values)
                for _ in 1:count
                    keybytes = spanlength!(cursor, cursor.limits.max_bytes, :max_bytes)
                    spanadvance!(cursor, keybytes)
                    spanvalue!(plan.values, cursor)
                end
            else
                for _ in 1:count
                    keybytes = spanlength!(cursor, cursor.limits.max_bytes, :max_bytes)
                    spanadvance!(cursor, keybytes)
                    values = spanvalueadd(cursor, values,
                                          spanvalue!(plan.values, cursor))
                    checkspanvalues(cursor, values)
                end
            end
            size >= 0 && cursor.pos != blockend &&
                spanerror(cursor, "sized map block not exactly consumed")
        end
    finally
        leavespan!(cursor)
    end
    return values
end

function spanvalue!(plan::UnionSpan, cursor::SpanState)
    isempty(plan.branches) && spanerror(cursor, "empty union has no datum")
    countspanvalues!(cursor, 1)
    index = spanlong!(cursor)
    0 <= index < length(plan.branches) ||
        spanerror(cursor, "index $index out of range (0:$(length(plan.branches) - 1))")
    return checkspanvalues(cursor,
                           spanvalueadd(cursor, 1,
                                        spanvalue!(plan.branches[Int(index) + 1],
                                                   cursor)))
end

"Scan a subtree whose value count is already charged by a static parent record."
function spanstatic!(plan::SpanPlan, cursor::SpanState)
    if plan isa IntSpan
        spanint!(cursor)
    elseif plan isa LongSpan
        spanlong!(cursor)
    elseif plan isa BytesSpan
        length = spanlength!(cursor, cursor.limits.max_bytes, :max_bytes)
        spanadvance!(cursor, length)
    elseif plan isa EnumSpan
        index = spanlong!(cursor)
        0 <= index < plan.symbols ||
            spanerror(cursor, "index $index out of range (0:$(plan.symbols - 1))")
    elseif plan isa FixedValueSpan
        checkspanfixed(cursor, plan.width)
        spanadvance!(cursor, plan.width)
    elseif plan isa FixedSpan
        spanadvance!(cursor, plan.width)
    elseif plan isa RecordSpan
        if plan.width >= 0
            checkspanfixed(cursor, plan.maxfixed)
            spanadvance!(cursor, plan.width)
            return nothing
        end
        plan.values >= 0 ||
            throw(ArgumentError("internal error: dynamic record in static span scan"))
        enterspan!(cursor)
        try
            for index in 1:plan.nsteps
                step = @inbounds plan.steps[index]
                if step.width >= 0
                    checkspanfixed(cursor, step.maxfixed)
                    spanadvance!(cursor, step.width)
                else
                    spanstatic!(step.plan::SpanPlan, cursor)
                end
            end
        finally
            leavespan!(cursor)
        end
    else
        throw(ArgumentError("internal error: $(typeof(plan)) has no static span scan"))
    end
    return nothing
end

function spanvalue!(plan::RecordSpan, cursor::SpanState)
    if plan.width >= 0
        countspanvalues!(cursor, plan.values)
        checkspanfixed(cursor, plan.maxfixed)
        spanadvance!(cursor, plan.width)
        return checkspanvalues(cursor, plan.values)
    end
    if plan.values >= 0
        countspanvalues!(cursor, plan.values)
        spanstatic!(plan, cursor)
        return checkspanvalues(cursor, plan.values)
    end
    enterspan!(cursor)
    values = plan.basevalues
    countspanvalues!(cursor, values)
    checkspanvalues(cursor, values)
    try
        for index in 1:plan.nsteps
            step = @inbounds plan.steps[index]
            if step.width >= 0
                countspanvalues!(cursor, step.values)
                checkspanfixed(cursor, step.maxfixed)
                spanadvance!(cursor, step.width)
                values = spanvalueadd(cursor, values, step.values)
            else
                values = spanvalueadd(cursor, values,
                                      spanvalue!(step.plan::SpanPlan, cursor))
            end
            checkspanvalues(cursor, values)
        end
    finally
        leavespan!(cursor)
    end
    return values
end

struct DatumSpan
    next::Int
    bytes::Int
    values::Int
    blind::Bool
end

"Synthetic values shared by every datum in one consumer operation."
struct SyntheticScope{D<:Decoder}
    state::D
end

@inline function syntheticvalues(scope::SyntheticScope)
    return scope.state.scopevalues
end

@inline function setsyntheticvalues!(scope::SyntheticScope, values::Int)
    scope.state.scopevalues = values
    return values
end

struct SyntheticCounter{D<:Decoder,S}
    budget::Budget
    datumbytes::Int
    basevalues::Int
    initial::Int
    countwriter::Bool
    state::D
    scope::S
end

function SyntheticCounter(budget::Budget, datumbytes::Int, basevalues::Int,
                          initial::Int, countwriter::Bool,
                          state::D, scope::S, values::Int) where {D<:Decoder,S}
    state.values = values
    return SyntheticCounter{D,S}(budget, datumbytes, basevalues, initial,
                                 countwriter, state, scope)
end

function SyntheticCounter(budget::Budget, datumbytes::Int, basevalues::Int,
                          initial::Int, countwriter::Bool,
                          state::Decoder, values::Int)
    return SyntheticCounter(budget, datumbytes, basevalues, initial,
                            countwriter, state, nothing, values)
end

@inline function syntheticvalues(counter::SyntheticCounter)
    return counter.state.values
end

@inline function setsyntheticvalues!(counter::SyntheticCounter, values::Int)
    counter.state.values = values
    return values
end

"Add synthetic values and fail closed on an observed-count overflow."
function syntheticvalueadd(counter::SyntheticCounter, a::Int, b::Int)
    a >= 0 && b >= 0 || throw(ArgumentError("synthetic values must be non-negative"))
    maximum = counter.budget.limits.max_total_values
    b <= typemax(Int) - a ||
        throw(limiterror(counter.budget, :max_total_values, typemax(Int), maximum))
    values = a + b
    values <= maximum ||
        throw(limiterror(counter.budget, :max_total_values, values, maximum))
    return values
end

"Admit reader-created values against the exact datum and shared operation work scopes."
function countsynthetic!(counter::SyntheticCounter, count::Int)
    count >= 0 || throw(ArgumentError("synthetic value count must be non-negative"))
    budget = counter.budget
    limits = budget.limits
    values = syntheticvalueadd(counter, syntheticvalues(counter), count)
    datumcap = muladdcap(limits.max_values_per_byte, counter.datumbytes,
                        limits.work_allowance)
    values <= datumcap ||
        throw(limiterror(budget, :max_values_per_byte, values, datumcap))
    checkblockspanvalues!(counter.state, values, counter.basevalues)
    scope = counter.scope
    operationvalues = scope === nothing ? values :
        syntheticvalueadd(counter, syntheticvalues(scope), count)
    total = syntheticvalueadd(counter, counter.basevalues, operationvalues)
    operationcap = muladdcap(limits.max_values_per_byte, budget.input_bytes,
                             limits.work_allowance)
    total <= operationcap ||
        throw(limiterror(budget, :max_values_per_byte, total, operationcap))
    setsyntheticvalues!(counter, values)
    scope === nothing || setsyntheticvalues!(scope, operationvalues)
    return count
end

"Count writer values only during the consumer-aware fast pre-scan."
function countwriter!(counter::SyntheticCounter, count::Int=1)
    counter.countwriter || return 0
    countsynthetic!(counter, count)
    return count
end

"Exact writer values that an inactive sized array block can hide without hiding a hard limit."
function fastskipvalues(::Union{NullPlan,BoolPlan,IntPlan,LongPlan,FloatPlan,
                                DoublePlan,EnumPlan,DatePlan,TimestampPlan,
                                LocalTimestampPlan}, limits::Limits)
    return 1
end

function fastskipvalues(plan::FixedPlan, limits::Limits)
    return plan.schema.size <= limits.max_bytes ? 1 : -1
end

function fastskipvalues(::UUIDFixedPlan, limits::Limits)
    return 16 <= limits.max_bytes ? 1 : -1
end

function fastskipvalues(::DurationPlan, limits::Limits)
    return 12 <= limits.max_bytes ? 1 : -1
end

function fastskipvalues(plan::UnionPlan, limits::Limits)
    isempty(plan.branches) && return -1
    branchvalues = fastskipvalues(first(plan.branches), limits)
    branchvalues >= 0 || return -1
    for index in 2:length(plan.branches)
        fastskipvalues(plan.branches[index], limits) == branchvalues || return -1
    end
    return checked_add(1, branchvalues)
end

function fastskipvalues(::ReadPlan, limits::Limits)
    return -1
end

"Skip one inactive value whose exact value count is known, without domain validation."
function fastskipvalue!(::NullPlan, decoder::Decoder)
    return nothing
end

function fastskipvalue!(::BoolPlan, decoder::Decoder)
    readbyte(decoder)
    return nothing
end

function fastskipvalue!(::Union{IntPlan,DatePlan}, decoder::Decoder)
    readint(decoder)
    return nothing
end

function fastskipvalue!(::Union{LongPlan,TimestampPlan,LocalTimestampPlan},
                        decoder::Decoder)
    readlong(decoder)
    return nothing
end

function fastskipvalue!(::FloatPlan, decoder::Decoder)
    readfloat(decoder)
    return nothing
end

function fastskipvalue!(::DoublePlan, decoder::Decoder)
    readdouble(decoder)
    return nothing
end

function fastskipvalue!(::EnumPlan, decoder::Decoder)
    readlong(decoder)
    return nothing
end

function fastskipvalue!(plan::FixedPlan, decoder::Decoder)
    return skipfixed(decoder, plan.schema.size)
end

function fastskipvalue!(::UUIDFixedPlan, decoder::Decoder)
    return skipfixed(decoder, 16)
end

function fastskipvalue!(::DurationPlan, decoder::Decoder)
    return skipfixed(decoder, 12)
end

function fastskipvalue!(plan::UnionPlan, decoder::Decoder)
    index = readindex(decoder, length(plan.branches))
    return fastskipvalue!(plan.branches[index], decoder)
end

"Count a declared run of values without leaking attacker-controlled multiplication overflow."
function countwriterproduct!(counter::SyntheticCounter, count::Int, values::Int)
    count >= 0 && values >= 0 || throw(ArgumentError("writer values must be non-negative"))
    count == 0 && return 0
    maximum = counter.budget.limits.max_total_values
    values <= div(maximum, count) ||
        throw(limiterror(counter.budget, :max_total_values, typemax(Int), maximum))
    product = count * values
    countwriter!(counter, product)
    return product
end

"Walk one plain read-plan value while counting only reader-created zero-input values."
function syntheticvalue!(plan::ReadPlan, decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    values = countwriter!(counter)
    structuralskipvalue(plan, decoder)
    return values
end

function syntheticvalue!(plan::ArrayPlan, decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    enter!(decoder)
    values = countwriter!(counter)
    try
        while true
            count, size = readblockcount(decoder)
            count == 0 && break
            outerstop = decoder.stop
            blocknext = -1
            if size >= 0
                blocknext = checked_add(decoder.pos, size)
                stop = blocknext - 1
                stop <= decoder.stop || dataerror(decoder, "sized array block exceeds the remaining bytes")
                itemvalues = active || !counter.countwriter ? 0 :
                    fastskipvalues(plan.items, decoder.budget.limits)
                if !active && (!counter.countwriter || itemvalues >= 0)
                    skipped = countwriterproduct!(counter, count, itemvalues)
                    values = syntheticvalueadd(counter, values, skipped)
                    decoder.pos = blocknext
                    continue
                end
                decoder.stop = stop
            end
            try
                for _ in 1:count
                    values = syntheticvalueadd(counter, values,
                                               syntheticvalue!(plan.items, decoder,
                                                               active, counter))
                end
                size >= 0 && decoder.pos != blocknext &&
                    dataerror(decoder, "sized array block not exactly consumed")
            finally
                size >= 0 && (decoder.stop = outerstop)
            end
        end
    finally
        leave!(decoder)
    end
    return values
end

function syntheticvalue!(plan::MapPlan, decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    enter!(decoder)
    values = countwriter!(counter)
    try
        while true
            count, size = readblockcount(decoder)
            count == 0 && break
            outerstop = decoder.stop
            blocknext = -1
            fastvalues = -1
            if size >= 0
                blocknext = checked_add(decoder.pos, size)
                stop = blocknext - 1
                stop <= decoder.stop || dataerror(decoder, "sized map block exceeds the remaining bytes")
                if !active && counter.countwriter
                    fastvalues = fastskipvalues(plan.values,
                                                decoder.budget.limits)
                    if fastvalues >= 0
                        skipped = countwriterproduct!(counter, count,
                                                      fastvalues)
                        values = syntheticvalueadd(counter, values, skipped)
                    end
                end
                decoder.stop = stop
            end
            try
                for _ in 1:count
                    skiplen(decoder)
                    if fastvalues >= 0
                        fastskipvalue!(plan.values, decoder)
                    else
                        values = syntheticvalueadd(
                            counter, values,
                            syntheticvalue!(plan.values, decoder, active,
                                            counter))
                    end
                end
                size >= 0 && decoder.pos != blocknext &&
                    dataerror(decoder, "sized map block not exactly consumed")
            finally
                size >= 0 && (decoder.stop = outerstop)
            end
        end
    finally
        leave!(decoder)
    end
    return values
end

function syntheticvalue!(plan::UnionPlan, decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    isempty(plan.branches) && dataerror(decoder, "empty union has no datum")
    values = countwriter!(counter)
    index = readindex(decoder, length(plan.branches))
    return syntheticvalueadd(counter, values,
                             syntheticvalue!(plan.branches[index], decoder,
                                             active, counter))
end

function syntheticvalue!(plan::RecordPlan, decoder::Decoder, active::Bool,
                         counter::SyntheticCounter)
    enter!(decoder)
    values = countwriter!(counter)
    try
        for field in plan.fields
            values = syntheticvalueadd(counter, values,
                                       syntheticvalue!(field, decoder, active,
                                                       counter))
        end
    finally
        leave!(decoder)
    end
    return values
end

"The root projection form; resolving records add only selected defaults."
function projectedsyntheticvalue!(plan, decoder::Decoder, projection,
                                  counter::SyntheticCounter)
    return syntheticvalue!(plan, decoder, true, counter)
end

"Add active reader-default values to a proven span in reusable decoder storage."
function syntheticdatumspan!(plan, decoder::Decoder, span::DatumSpan,
                             projection=nothing)
    position = decoder.pos
    outerstop = decoder.stop
    depth = decoder.depth
    decoder.stop = span.next - 1
    decoder.depth = 0
    try
        counter = SyntheticCounter(decoder.budget, span.bytes,
                                   decoder.budget.values, span.values, false,
                                   decoder, span.values)
        countsynthetic!(counter, 0)
        added = projection === nothing ?
            syntheticvalue!(plan, decoder, true, counter) :
            projectedsyntheticvalue!(plan, decoder, projection, counter)
        decoder.pos == span.next ||
            throw(DataError("synthetic-value scan did not consume the proven datum span",
                            decoder.pos))
        added == syntheticvalues(counter) - counter.initial ||
            throw(DataError("internal error: synthetic value count mismatch",
                            decoder.pos))
        decoder.start = span.next
        decoder.datummax = span.bytes
        decoder.values = syntheticvalues(counter)
        decoder.blind = false
    finally
        decoder.pos = position
        decoder.stop = outerstop
        decoder.depth = depth
    end
    return nothing
end

"Pre-admit all writer values and active defaults under the actual fast-mode consumer plan."
function consumerdatumspan!(plan, decoder::Decoder, span::DatumSpan,
                            projection=nothing; active::Bool=true,
                            basevalues::Int=decoder.budget.values)
    decoder.validate === :fast ||
        throw(ArgumentError("consumer scan requires validate=:fast"))
    position = decoder.pos
    outerstop = decoder.stop
    depth = decoder.depth
    decoder.stop = span.next - 1
    decoder.depth = 0
    try
        counter = SyntheticCounter(decoder.budget, span.bytes, basevalues, 0,
                                   true, decoder, 0)
        countsynthetic!(counter, 0)
        added = projection === nothing ?
            syntheticvalue!(plan, decoder, active, counter) :
            projectedsyntheticvalue!(plan, decoder, projection, counter)
        decoder.pos == span.next ||
            throw(DataError("consumer-value scan did not consume the proven datum span",
                            decoder.pos))
        added == syntheticvalues(counter) ||
            throw(DataError("internal error: consumer value count mismatch",
                            decoder.pos))
        decoder.start = span.next
        decoder.datummax = span.bytes
        decoder.values = syntheticvalues(counter)
        decoder.blind = false
    finally
        decoder.pos = position
        decoder.stop = outerstop
        decoder.depth = depth
    end
    return nothing
end

"Apply reader defaults in strict mode or the complete consumer-aware pre-scan in fast mode."
function planneddatumspan!(plan, decoder::Decoder, span::DatumSpan,
                           resolved::Bool, projection, active::Bool,
                           basevalues::Int)
    if decoder.validate === :fast && (span.blind || !active)
        return consumerdatumspan!(plan, decoder, span, projection;
                                  active=active, basevalues=basevalues)
    elseif resolved
        return syntheticdatumspan!(plan, decoder, span, projection)
    end
    decoder.start = span.next
    decoder.datummax = span.bytes
    decoder.values = span.values
    decoder.blind = span.blind
    return nothing
end

function planneddatumspan!(plan, decoder::Decoder, span::DatumSpan;
                           resolved::Bool=false, projection=nothing,
                           active::Bool=true,
                           basevalues::Int=decoder.budget.values)
    return planneddatumspan!(plan, decoder, span, resolved, projection,
                             active, basevalues)
end

function planneddatumspan(plan, decoder::Decoder, span::DatumSpan; kwargs...)
    planneddatumspan!(plan, decoder, span; kwargs...)
    return decoderspan(decoder)
end

"The largest possible datum suffix available to one span pass."
function datumspanmax(buffer::AbstractVector{UInt8}, start::Int, limits::Limits)
    1 <= start <= length(buffer) + 1 || throw(ArgumentError("position $start out of range"))
    return min(limits.max_datum_bytes, length(buffer) - start + 1)
end

"The operation input upper bound while a raw datum has not yet been credited."
function rawspaninputmax(budget::Budget, buffer::AbstractVector{UInt8}, start::Int,
                         limits::Limits)
    return satadd(budget.input_bytes, datumspanmax(buffer, start, limits))
end

"Find one exact writer datum starting at `start` without materialising it."
function datumspan(plan::SpanPlan, buffer::AbstractVector{UInt8}, start::Int, limits::Limits;
                   values0::Int=0, inputmax::Union{Nothing,Int}=nothing,
                   validate::Symbol=:strict)
    datummax = datumspanmax(buffer, start, limits)
    values0 >= 0 || throw(ArgumentError("prior span values must be non-negative"))
    validate in (:strict, :fast) ||
        throw(ArgumentError("validate must be :strict or :fast"))
    maximum = inputmax === nothing ? datummax : inputmax
    maximum >= 0 || throw(ArgumentError("maximum span input must be non-negative"))
    cursor = SpanCursor(buffer, start, length(buffer), start, limits,
                        datummax, values0, maximum, validate, 0, 0, false)
    values = spanvalue!(plan, cursor)
    checkspanvalues(cursor, values)
    values == cursor.values ||
        throw(DataError("internal error: span value count mismatch", cursor.pos))
    return DatumSpan(cursor.pos, cursor.pos - start, values, cursor.blind)
end

"Find one exact writer datum with the caller's reusable decoder as span-cursor storage."
function datumspan!(plan::SpanPlan, decoder::Decoder, start::Int,
                    values0::Int, inputmax::Int)
    limits = decoder.limits
    1 <= start <= decoder.stop + 1 ||
        throw(ArgumentError("position $start out of range"))
    values0 >= 0 || throw(ArgumentError("prior span values must be non-negative"))
    inputmax >= 0 || throw(ArgumentError("maximum span input must be non-negative"))
    datummax = min(limits.max_datum_bytes, decoder.stop - start + 1)
    position = decoder.pos
    depth = decoder.depth
    decoder.start = start
    decoder.pos = start
    decoder.datummax = datummax
    decoder.values0 = values0
    decoder.inputmax = inputmax
    decoder.values = 0
    decoder.depth = 0
    decoder.blind = false
    try
        values = spanvalue!(plan, decoder)
        checkspanvalues(decoder, values)
        values == decoder.values ||
            throw(DataError("internal error: span value count mismatch", decoder.pos))
        next = decoder.pos
        decoder.start = next
        decoder.datummax = next - start
        decoder.values = values
    finally
        decoder.pos = position
        decoder.depth = depth
    end
    return nothing
end

@inline function decoderspan(decoder::Decoder)
    return DatumSpan(decoder.start, decoder.datummax, decoder.values,
                     decoder.blind)
end

function datumspan(plan::SpanPlan, decoder::Decoder, start::Int,
                   values0::Int, inputmax::Int)
    datumspan!(plan, decoder, start, values0, inputmax)
    return decoderspan(decoder)
end

struct DatumWork
    span::DatumSpan
    values0::Int
    outerstop::Int
end

struct StaticDatumWork
    start::Int
    values0::Int
    outerstop::Int
    expected::Int
    outerlimitactive::Bool
end

"A static value count that can never consume the shared allowance by itself."
function staticdatumvalues(plan::SpanPlan, limits::Limits)
    values = spanvalues(plan)
    return 0 <= values <= limits.work_allowance ? values : -1
end

"Pre-admit a static datum and enforce its byte ceiling through the live decoder stop."
function beginstaticdatum!(decoder::Decoder, expected::Int)
    0 <= expected <= decoder.limits.work_allowance ||
        throw(ArgumentError("static datum values exceed the work allowance"))
    checkblockspanvalues!(decoder, expected, decoder.budget.values)
    checkprojectedvalues!(decoder.budget, expected)
    start = decoder.pos
    outerstop = decoder.stop
    limitstop = min(outerstop,
                    satadd(start - 1, decoder.limits.max_datum_bytes))
    work = StaticDatumWork(start, decoder.budget.values, outerstop, expected,
                           decoder.datumlimitactive)
    decoder.start = start
    decoder.datumlimitactive = true
    decoder.stop = limitstop
    return work
end

"Commit a statically admitted datum after its real decode established the exact byte count."
function finishstaticdatum!(decoder::Decoder, work::StaticDatumWork;
                            credit::Bool=false)
    try
        bytes = decoder.pos - work.start
        credit && addinput!(decoder.budget, bytes)
        actual = decoder.budget.values - work.values0
        actual == work.expected ||
            throw(DataError("internal error: static datum value count mismatch", decoder.pos))
        checkworkscope!(decoder.budget, actual, bytes)
        checkoperationwork!(decoder.budget)
        return bytes
    finally
        decoder.stop = work.outerstop
        decoder.datumlimitactive = work.outerlimitactive
    end
end

function abortdatum!(decoder::Decoder, work::StaticDatumWork)
    decoder.stop = work.outerstop
    decoder.datumlimitactive = work.outerlimitactive
    return nothing
end

"Pre-admit one exact datum and restrict its real decoder to the proven span."
function begindatum!(decoder::Decoder, plan::SpanPlan, limits::Limits)
    decoder.validate === :fast &&
        throw(ArgumentError("fast datum admission requires the consumer plan"))
    limits == decoder.limits ||
        throw(ArgumentError("datum limits do not match the decoder"))
    datumspan!(plan, decoder, decoder.pos, decoder.budget.values,
               decoder.budget.input_bytes)
    span = decoderspan(decoder)
    return begindatum!(decoder, span)
end

function begindatum!(decoder::Decoder, spanplan::SpanPlan, consumer, limits::Limits;
                     resolved::Bool=false, projection=nothing, active::Bool=true)
    limits == decoder.limits ||
        throw(ArgumentError("datum limits do not match the decoder"))
    datumspan!(spanplan, decoder, decoder.pos, decoder.budget.values,
               decoder.budget.input_bytes)
    span = decoderspan(decoder)
    planneddatumspan!(consumer, decoder, span; resolved=resolved,
                      projection=projection, active=active)
    span = decoderspan(decoder)
    return begindatum!(decoder, span)
end

function begindatum!(decoder::Decoder, span::DatumSpan)
    span.next - 1 <= decoder.stop ||
        throw(DataError("datum span exceeds the decoder boundary", decoder.pos))
    checkworkscope!(decoder.budget, span.values, span.bytes; projected=true)
    work = DatumWork(span, decoder.budget.values, decoder.stop)
    decoder.stop = span.next - 1
    return work
end

"Commit the exact value count of a decoded/skipped datum and restore the enclosing cursor stop."
function finishdatum!(decoder::Decoder, work::DatumWork)
    try
        decoder.pos == work.span.next ||
            throw(DataError("datum did not consume its proven encoded span", decoder.pos))
        actual = decoder.budget.values - work.values0
        if actual < work.span.values
            countvalues!(decoder.budget, work.span.values - actual)
            actual = work.span.values
        end
        checkworkscope!(decoder.budget, actual, work.span.bytes)
        checkoperationwork!(decoder.budget)
        return actual
    finally
        decoder.stop = work.outerstop
    end
end

function abortdatum!(decoder::Decoder, work::DatumWork)
    decoder.stop = work.outerstop
    return nothing
end

struct PositionalDatumSource{B<:AbstractVector{UInt8}}
    buffer::B
    start::Int
    base::Int
    sourceend::Int
end

"Normalise only the positional window that one datum can consume; retain original coordinates."
function positionalsource(source::AbstractVector{UInt8}, position::Int,
                          limits::Limits, budget::Budget)
    1 <= position <= length(source) + 1 ||
        throw(ArgumentError("position $position out of range"))
    if source isa Vector{UInt8} ||
       (source isa SubArray && parent(source) isa Vector{UInt8} && Base.iscontiguous(source))
        return PositionalDatumSource(source, position, 0, length(source))
    end
    remaining = length(source) - position + 1
    extra = limits.max_datum_bytes == typemax(Int) ? 0 : 1
    count = min(remaining, checked_add(limits.max_datum_bytes, extra))
    charge = bytesbytes(count)
    reserve!(budget, charge)
    buffer = Vector{UInt8}(undef, count)
    allocated!(budget, charge)
    copyto!(buffer, 1, source, position, count)
    return PositionalDatumSource(buffer, 1, position - 1, length(source))
end

function originalnext(source::PositionalDatumSource, localnext::Int)
    return checked_add(source.base, localnext)
end
