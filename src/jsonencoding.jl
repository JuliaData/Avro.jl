# JSON encoding (plan §4.11). `tojson` validates through the binary write plan (identical validation and
# union branch recovery to `encode`), decodes the bytes back into the closed generic value set and prints
# that with Java-compatible labels; `fromjson` parses with Avro's own bounded JSON reader (never JSON.jl)
# and converts by the §4.11 rule table. Both are bounded by `max_datum_bytes`, `max_total_values`,
# `max_json_depth` and the operation budget.

struct JSONOut
    io::BoundedWriter
    maxbytes::Int
    pretty::Bool
end

function checkoutput!(out::JSONOut)
    n = out.io.len
    n <= out.maxbytes || throw(LimitError(:max_datum_bytes, n, out.maxbytes, :max_datum_bytes, :encode))
    return nothing
end

"""
    Avro.tojson(schema, x; pretty=false, limits=Limits()) -> String

The Avro JSON encoding of `x` under `schema` (Java-compatible: unions as one-member objects keyed by
the branch's fullname or type name, bytes/fixed as strings of code points U+0000–U+00FF, non-finite
floats as the strings `"NaN"`, `"Infinity"`, `"-Infinity"`). Validation and union branch recovery are
those of `Avro.encode`; the output text is bounded by `max_datum_bytes`.
"""
function tojson(s::Schema, x; pretty::Bool=false, limits::Limits=Limits())
    graphinfo(s).repaired_names &&
        throw(ArgumentError("tojson rejects a schema parsed with allow_invalid_names=true"))
    return withbudget(limits; direction=:encode) do budget
        local text
        beginworkdefer!(budget)
        try
            writer = writeplan(s; budget=budget)
            encoder = Encoder(budget)
            withencoderroot!(encoder, s) do
                encode(writer, encoder, x)
            end
            d = Decoder(encoder.buf, budget; stop=encoder.pos)
            v = decode(readplan(s; budget=budget), d)
            d.pos == encoder.pos + 1 || throw(DataError("internal error: encoded datum was not fully consumed", d.pos))
            sink = BoundedWriter(budget, limits.max_datum_bytes;
                                 limit=:max_datum_bytes)
            out = JSONOut(sink, limits.max_datum_bytes, pretty)
            printvalue(out, s, v, 0, budget)
            text = boundedtake!(sink)
        finally
            endworkdefer!(budget)
        end
        checkoperationwork!(budget)
        checkcomparisonwork!(budget)
        return text
    end
end

function printvalue(out::JSONOut, s::Schema, v, depth::Int, budget::Budget)
    countvalues!(budget)
    depth <= budget.limits.max_json_depth || throw(LimitError(:max_json_depth, depth, budget.limits.max_json_depth, :max_json_depth, :encode))
    printkind(out, s, v, depth, budget)
    checkoutput!(out)
    return nothing
end

function printkind(out::JSONOut, ::NullSchema, v, depth, budget)
    return print(out.io, "null")
end

function printkind(out::JSONOut, ::BooleanSchema, v::Bool, depth, budget)
    return print(out.io, v ? "true" : "false")
end

function printkind(out::JSONOut, ::Union{FloatSchema,DoubleSchema}, v::AbstractFloat, depth, budget)
    return printfloat(out.io, v, budget)
end

function printkind(out::JSONOut, ::StringSchema, v::String, depth, budget)
    return escapejson(out.io, v)
end

function printkind(out::JSONOut, ::StringSchema, v::UUID, depth, budget)
    return printuuidstring(out.io, v)
end

function printkind(out::JSONOut, ::EnumSchema, v::EnumValue, depth, budget)
    return escapejson(out.io, String(v))
end

function printkind(out::JSONOut, ::BytesSchema, v::Vector{UInt8}, depth, budget)
    return printbytestring(out.io, v)
end

function printkind(out::JSONOut, ::BytesSchema, v::WideDecimal, depth, budget)
    bytes = twoscomplement(v.unscaled, budget)
    try
        return printbytestring(out.io, bytes)
    finally
        release!(budget, bytesbytes(length(bytes)))
    end
end

function printkind(out::JSONOut, s::IntSchema, v, depth, budget)
    l = s.logical
    n = l isa DateLogical ? Dates.value(v::Date - DATE_EPOCH) :
        l isa TimeMillis ? Dates.value(v::Time) ÷ 1_000_000 : Int(v::Int32)
    return print(out.io, n)
end

function printkind(out::JSONOut, s::LongSchema, v, depth, budget)
    l = s.logical
    n = l isa TimeMicros ? Dates.value(v::Time) ÷ 1_000 :
        (l isa Union{TimestampMillis,TimestampMicros,TimestampNanos,LocalTimestampMillis,LocalTimestampMicros,LocalTimestampNanos} ? v.ticks : v::Int64)
    return print(out.io, n)
end

function printkind(out::JSONOut, s::FixedSchema, v, depth, budget)
    checkvaluebytes(budget, s.size)
    v isa Fixed && return printbytestring(out.io, v.bytes)
    v isa UUID && return printuuidbytestring(out.io, v)
    v isa Durations.Duration && (v = Duration(v))
    v isa DataDecimals.AbstractDecimal && !(v isa Decimal) && (v = _decimalinput(v))
    v isa Duration && return printdurationbytestring(out.io, v)
    bytes = twoscomplement(v.unscaled, budget; maxbytes=s.size)
    try
        length(bytes) <= s.size ||
            throw(EncodeError("decimal does not fit the fixed size $(s.size)", "\$", s))
        return printpaddedbytestring(out.io, bytes, s.size, v.unscaled < 0 ? 0xff : 0x00)
    finally
        release!(budget, bytesbytes(length(bytes)))
    end
end

function printkind(out::JSONOut, s::ArraySchema, v::AbstractVector, depth, budget)
    io = out.io
    print(io, '[')
    for (i, x) in enumerate(v)
        i > 1 && print(io, ',')
        indent(io, out.pretty, depth + 1)
        printvalue(out, s.items, x, depth + 1, budget)
    end
    isempty(v) || indent(io, out.pretty, depth)
    print(io, ']')
    return nothing
end

function printkind(out::JSONOut, s::MapSchema, v::Map, depth, budget)
    io = out.io
    print(io, '{')
    for i in eachindex(v.keys)
        i > 1 && print(io, ',')
        indent(io, out.pretty, depth + 1)
        escapejson(io, v.keys[i])
        print(io, out.pretty ? ": " : ":")
        printvalue(out, s.values, v.vals[i], depth + 1, budget)
    end
    isempty(v.keys) || indent(io, out.pretty, depth)
    print(io, '}')
    return nothing
end

function printkind(out::JSONOut, s::RecordSchema, v::Record, depth, budget)
    io = out.io
    vals = getfield(v, :values)
    print(io, '{')
    for (i, f) in enumerate(s.fields)
        i > 1 && print(io, ',')
        indent(io, out.pretty, depth + 1)
        escapejson(io, f.name)
        print(io, out.pretty ? ": " : ":")
        printvalue(out, f.schema, vals[i], depth + 1, budget)
    end
    isempty(s.fields) || indent(io, out.pretty, depth)
    print(io, '}')
    return nothing
end

function printkind(out::JSONOut, s::UnionSchema, v, depth, budget)
    nb = nullablebranch(s)
    if nb != 0
        if v === missing
            countvalues!(budget)
            return print(out.io, "null")
        end
        return printmember(out, s, 3 - nb, v, depth, budget)
    end
    u = v::UnionValue
    branch = s.branches[u.index]
    if branch isa NullSchema
        countvalues!(budget)
        return print(out.io, "null")
    end
    return printmember(out, s, u.index, u.value, depth, budget)
end

function printmember(out::JSONOut, s::UnionSchema, i::Int, v, depth, budget)
    branch = s.branches[i]
    ambiguouslabel(s, i, budget) &&
        throw(EncodeError("union label is ambiguous (a named type and a kind share it)", "\$", s))
    io = out.io
    print(io, '{')
    indent(io, out.pretty, depth + 1)
    escapeunionlabel(io, branch)
    print(io, out.pretty ? ": " : ":")
    printvalue(out, branch, v, depth + 1, budget)
    indent(io, out.pretty, depth)
    print(io, '}')
    return nothing
end

function unionlabel(s::NamedSchema)
    return fullname(s)
end

function unionlabel(s::Schema)
    return unionkindlabel(s)
end

function unionkindlabel(s::Schema)
    k = kind(s)
    k === :null && return "null"
    k === :boolean && return "boolean"
    k === :int && return "int"
    k === :long && return "long"
    k === :float && return "float"
    k === :double && return "double"
    k === :bytes && return "bytes"
    k === :string && return "string"
    k === :array && return "array"
    k === :map && return "map"
    throw(ArgumentError("internal error: a union label has no schema kind"))
end

function stringcomparisonwork(a::AbstractString, b::AbstractString)
    aa = codeunits(a)
    bb = codeunits(b)
    n = min(length(aa), length(bb))
    for i in 1:n
        aa[i] == bb[i] || return i
    end
    return n + 1
end

function unionlabelmatches(s::NamedSchema, label::String, budget::Budget)
    addcompare!(budget, fullnamecomparisonwork(s.name, label))
    return fullnameequal(s.name, label)
end

function unionlabelmatches(s::Schema, label::String, budget::Budget)
    expected = unionkindlabel(s)
    addcompare!(budget, stringcomparisonwork(expected, label))
    return expected == label
end

function sameunionlabel(a::NamedSchema, b::NamedSchema, budget::Budget)
    return budgetedfullnameequal(a.name, b.name, budget)
end

function sameunionlabel(a::NamedSchema, b::Schema, budget::Budget)
    return budgetedfullnameequal(a.name, unionkindlabel(b), budget)
end

function sameunionlabel(a::Schema, b::NamedSchema, budget::Budget)
    return sameunionlabel(b, a, budget)
end

function sameunionlabel(a::Schema, b::Schema, budget::Budget)
    x = unionkindlabel(a)
    y = unionkindlabel(b)
    addcompare!(budget, stringcomparisonwork(x, y))
    return x == y
end

function ambiguouslabel(s::UnionSchema, index::Int, budget::Budget)
    selected = s.branches[index]
    for (i, branch) in enumerate(s.branches)
        i == index && continue
        sameunionlabel(selected, branch, budget) && return true
    end
    return false
end

function branchbylabel(s::UnionSchema, label::String, budget::Budget)
    found = 0
    for (i, b) in enumerate(s.branches)
        unionlabelmatches(b, label, budget) || continue
        found == 0 || return -1
        found = i
    end
    return found
end

function escapeunionlabel(io::IO, s::NamedSchema)
    print(io, '"')
    if !isempty(s.name.namespace)
        escapejsoncontents(io, s.name.namespace)
        print(io, '.')
    end
    escapejsoncontents(io, s.name.name)
    return print(io, '"')
end

function escapeunionlabel(io::IO, s::Schema)
    return escapejson(io, unionkindlabel(s))
end

function printfloat(io::IO, v::AbstractFloat, budget::Budget)
    isfinite(v) || return print(io, isnan(v) ? "\"NaN\"" : (v > 0 ? "\"Infinity\"" : "\"-Infinity\""))
    capacity = Base.Ryu.neededdigits(typeof(v))
    charge = bytesbytes(capacity)
    reserve!(budget, charge)
    bytes = try
        buffer = Vector{UInt8}(undef, capacity)
        allocated!(budget, charge)
        buffer
    catch
        unreserve!(budget, charge)
        rethrow()
    end
    try
        stop = Base.Ryu.writeshortest(bytes, 1, v)
        GC.@preserve bytes Base.unsafe_write(io, pointer(bytes), UInt(stop - 1))
        return nothing
    finally
        release!(budget, charge)
    end
end

# Bytes as a JSON string of code points U+0000–U+00FF (Java's form).
function printbytestring(io::IO, bytes::AbstractVector{UInt8})
    print(io, '"')
    for b in bytes
        printjsonbyte(io, b)
    end
    print(io, '"')
    return nothing
end

function printjsonbyte(io::IO, b::UInt8)
    if b == UInt8('"')
        print(io, "\\\"")
    elseif b == UInt8('\\')
        print(io, "\\\\")
    elseif b < 0x20
        if b == 0x08; print(io, "\\b") elseif b == 0x0C; print(io, "\\f") elseif b == 0x0A; print(io, "\\n")
        elseif b == 0x0D; print(io, "\\r") elseif b == 0x09; print(io, "\\t")
        else writehexescape(io, b) end
    elseif b < 0x80
        Base.write(io, b)
    else
        Base.write(io, 0xC0 | (b >> 6), 0x80 | (b & 0x3F))
    end
    return nothing
end

"Print canonical lowercase UUID text as a JSON string without a temporary `String`."
function printuuidstring(io::IO, u::UUID)
    Base.write(io, UInt8('"'))
    value = UInt128(u)
    for i in 0:31
        if i == 8 || i == 12 || i == 16 || i == 20
            Base.write(io, UInt8('-'))
        end
        Base.write(io, HEX_DIGITS[Int((value >> (4 * (31 - i))) & 0x0f) + 1])
    end
    Base.write(io, UInt8('"'))
    return nothing
end

"Print the 16 big-endian UUID bytes as an Avro JSON bytes string without a temporary vector."
function printuuidbytestring(io::IO, u::UUID)
    Base.write(io, UInt8('"'))
    value = UInt128(u)
    for i in 0:15
        printjsonbyte(io, UInt8((value >> (8 * (15 - i))) & 0xff))
    end
    Base.write(io, UInt8('"'))
    return nothing
end

"Print the 12 little-endian duration bytes without a temporary vector."
function printdurationbytestring(io::IO, x::Duration)
    Base.write(io, UInt8('"'))
    for value in (x.months, x.days, x.millis)
        for i in 0:3
            printjsonbyte(io, UInt8((value >> (8 * i)) & 0xff))
        end
    end
    Base.write(io, UInt8('"'))
    return nothing
end

function printpaddedbytestring(io::IO, bytes::Vector{UInt8}, size::Int, pad::UInt8)
    print(io, '"')
    for _ in 1:size - length(bytes)
        printjsonbyte(io, pad)
    end
    for b in bytes
        printjsonbyte(io, b)
    end
    print(io, '"')
    return nothing
end

# ---- fromjson ---------------------------------------------------------------------------------------

struct JSONContext
    budget::Budget
    strict::Bool
    unknownerror::Bool
    bareunion::Bool        # defaults: unions are bare values matched in branch order (plan §4.2)
    countdatum::Bool
    branches::Union{Nothing,Vector{Int}}
    copystrings::Bool      # frozen defaults must materialise fresh caller-owned strings
end

function JSONContext(budget::Budget, strict::Bool, unknownerror::Bool,
                     bareunion::Bool)
    return JSONContext(budget, strict, unknownerror, bareunion, true, nothing, false)
end

"""
    Avro.fromjson(schema, json; strict=true, unknown=:ignore, limits=Limits(), names=Avro.DEFAULT_ADMISSION)
    Avro.fromjson(schema, json, T; kw...)

Decode the Avro JSON encoding of one datum (`json` is a string, bytes or an `IO`) into the generic value
model, or into `T` through the semantic StructUtils route. `strict=false` additionally accepts the bare
tokens `NaN`/`Infinity`/`-Infinity`; `unknown=:error` rejects unknown record members (ignored by
default, like Java).
"""
function fromjson(s::Schema, src::Union{AbstractString,AbstractVector{UInt8},IO}; strict::Bool=true, unknown::Symbol=:ignore,
                  limits::Limits=Limits(), names=DEFAULT_ADMISSION)
    graphinfo(s).repaired_names &&
        throw(ArgumentError("fromjson rejects a schema parsed with allow_invalid_names=true"))
    unknown in (:ignore, :error) || throw(ArgumentError("unknown must be :ignore or :error"))
    admission(names)
    return withbudget(limits) do budget
        bytes = sourcebytes(src, limits.max_datum_bytes, budget, DataError)
        addinput!(budget, length(bytes))
        errfn = (msg, pos) -> throw(DataError(msg, pos))
        limitfn = (limit, observed, value) -> throw(LimitError(limit, observed, value, limit, :decode))
        j = parsejson(bytes; maxbytes=limits.max_datum_bytes, maxdepth=limits.max_json_depth, errfn=errfn, budget=budget,
                      limitfn=limitfn, bytelimit=:max_datum_bytes, depthlimit=:max_json_depth, lenient=!strict)
        return jsontovalue(s, j, JSONContext(budget, strict, unknown === :error, false), 1)
    end
end

function fromjson(s::Schema, src, ::Type{T}; names=DEFAULT_ADMISSION, kw...) where {T}
    v = fromjson(s, src; names=names, kw...)
    (T === Any || v isa T) && return v
    return semanticvalue(T, v, admission(names), s)
end

"""
    jsonvalue(schema, json, budget) -> value

Convert a frozen JSON tree parsed in *default* context (bare unions, the recursive record rule) into the
generic value model — used for field defaults under resolution.
"""
function jsonvalue(s::Schema, j, budget::Budget)
    return jsonvalue(s, j, budget, 0)
end

function jsonvalue(s::Schema, j, budget::Budget, branch::Int)
    work = defaultwork(s, j, branch, budget)
    try
        return jsonvalue(s, j, budget, branch, work.branches, true)
    finally
        bytes = defaultbranchbytes(work.branches)
        bytes > 0 && release!(budget, bytes)
    end
end

function jsonvalue(plan::DefaultPlan, budget::Budget; countdatum::Bool=true,
                   depth::Int=1)
    return jsonvalue(plan.schema, plan.json, budget, plan.branch, plan.branches,
                     countdatum, depth)
end

function jsonvalue(s::Schema, j, budget::Budget, branch::Int,
                   branches::Vector{Int}, countdatum::Bool, depth::Int=1)
    if branch != 0 && s isa UnionSchema
        isempty(branches) &&
            throw(DataError(diagnosticstring(budget,
                                             "resolved default branch tape is empty"), 0))
        branches[1] == branch ||
            throw(DataError(diagnosticstring(budget,
                                             "resolved default branch tape does not match its root"), 0))
    end
    position = budget.default_position
    compared = budget.compare_bytes
    budget.default_position = 1
    countdatum || beginworkdefer!(budget)
    try
        ctx = JSONContext(budget, true, false, true, countdatum, branches, true)
        value = jsontovalue(s, j, ctx, depth)
        budget.default_position == length(branches) + 1 ||
            jsonerror(ctx, "resolved default branch tape has trailing entries")
        return value
    finally
        countdatum || endworkdefer!(budget)
        countdatum || (budget.compare_bytes = compared)
        budget.default_position = position
    end
end

function jsonerror(msg::AbstractString)
    throw(DataError(msg, 0))
end

function jsonerror(ctx::JSONContext, parts...)
    throw(DataError(diagnosticstring(ctx.budget, parts...), 0))
end

function jsontovalue(s::Schema, j, ctx::JSONContext, depth::Int)
    if ctx.countdatum
        countvalues!(ctx.budget)
    else
        addresolution!(ctx.budget)
    end
    return jsonkind(s, j, ctx, depth)
end

function jsontovalue(s::Schema, j, ctx::JSONContext, depth::Int, branch::Int)
    ctx.branches === nothing || return jsontovalue(s, j, ctx, depth)
    (branch == 0 || !(s isa UnionSchema)) && return jsontovalue(s, j, ctx, depth)
    1 <= branch <= length(s.branches) ||
        throw(DataError(diagnosticstring(ctx.budget,
                                         "invalid default union branch ", branch), 0))
    countvalues!(ctx.budget)                          # the union root omitted by the selected-branch call
    v = jsontovalue(s.branches[branch], j, ctx, depth)
    return nullablebranch(s) != 0 ? v : chargedunion(branch, v, ctx)
end

function jsonkind(::NullSchema, j, ctx, depth)
    return j === nothing ? missing :
           jsonerror(ctx, "expected null, got ", describejson(j))
end

function jsonkind(::BooleanSchema, j, ctx, depth)
    return j isa Bool ? j :
           jsonerror(ctx, "expected a boolean, got ", describejson(j))
end

function jsonkind(s::IntSchema, j, ctx, depth)
    j isa Int64 || jsonerror(ctx, "expected an integer, got ", describejson(j))
    typemin(Int32) <= j <= typemax(Int32) ||
        jsonerror(ctx, "integer ", j, " does not fit int")
    l = s.logical
    l isa DateLogical && return DATE_EPOCH + Day(j)
    if l isa TimeMillis
        0 <= j < 86_400_000 ||
            jsonerror(ctx, "time-millis ", j, " is outside one day")
        return Time(Nanosecond(j * 1_000_000))
    end
    return Int32(j)
end

function jsonkind(s::LongSchema, j, ctx, depth)
    j isa Int64 || jsonerror(ctx, "expected an integer, got ", describejson(j))
    l = s.logical
    if l isa TimeMicros
        0 <= j < 86_400_000_000 ||
            jsonerror(ctx, "time-micros ", j, " is outside one day")
        return Time(Nanosecond(j * 1_000))
    end
    l isa TimestampMillis && return Timestamp{Millisecond}(j)
    l isa TimestampMicros && return Timestamp{Microsecond}(j)
    l isa TimestampNanos && return Timestamp{Nanosecond}(j)
    l isa LocalTimestampMillis && return LocalTimestamp{Millisecond}(j)
    l isa LocalTimestampMicros && return LocalTimestamp{Microsecond}(j)
    l isa LocalTimestampNanos && return LocalTimestamp{Nanosecond}(j)
    return j
end

function jsonkind(::FloatSchema, j, ctx, depth)
    return jsonfloat(Float32, j, ctx)
end

function jsonkind(::DoubleSchema, j, ctx, depth)
    return jsonfloat(Float64, j, ctx)
end

function jsonfloat(::Type{T}, j, ctx::JSONContext) where {T}
    j isa Int64 && return T(j)
    j isa Float64 && return T(j)                 # lenient non-finite tokens
    j isa JSONNumber && return parsefloat(T, j.text)
    if j isa String
        j == "NaN" && return T(NaN)
        j == "Infinity" && return T(Inf)
        j == "-Infinity" && return T(-Inf)
    end
    return jsonerror(ctx,
                     "expected a number (or \"NaN\"/\"Infinity\"/\"-Infinity\"), got ",
                     describejson(j))
end

function jsonkind(s::BytesSchema, j, ctx, depth)
    bytes = bytesfromjson(j, -1, ctx)
    l = s.logical
    if l isa DecimalLogical
        try
            return decimalfromvector(bytes, l, ctx)
        finally
            release!(ctx.budget, bytesbytes(length(bytes)))
        end
    end
    return bytes
end

function jsonkind(s::FixedSchema, j, ctx, depth)
    bytes = bytesfromjson(j, s.size, ctx)
    l = s.logical
    if l !== nothing
        try
            l isa DecimalLogical && return decimalfromvector(bytes, l, ctx)
            l isa UUIDLogical && return uuidfrombytes(bytes)
            l isa DurationLogical && return Duration(le32(bytes, 1), le32(bytes, 5), le32(bytes, 9))
        finally
            release!(ctx.budget, bytesbytes(length(bytes)))
        end
    end
    reserve!(ctx.budget, STORAGE[].fixed)
    value = Fixed(s, bytes, Val(:unchecked))
    allocated!(ctx.budget, STORAGE[].fixed)
    return value
end

function jsonkind(s::StringSchema, j, ctx, depth)
    j isa String || jsonerror(ctx, "expected a string, got ", describejson(j))
    isstrictutf8(j) ||
        jsonerror(ctx, "string is not valid UTF-8 (lone surrogate escape)")
    if s.logical isa UUIDLogical
        u = tryparseuuid(j)
        u === nothing &&
            jsonerror(ctx, "not an RFC 4122 uuid string: ", boundedquoted(j))
        return u
    end
    if ctx.copystrings
        checkvaluebytes(ctx.budget, sizeof(j))
        return ownedstringcopy(j, ctx.budget)
    end
    return j                                           # transfer the parser-owned immutable string
end

function jsonkind(s::EnumSchema, j, ctx, depth)
    j isa String ||
        jsonerror(ctx, "expected an enum symbol string, got ", describejson(j))
    budgetedhaskey(s.symbolindex, j, ctx.budget) ||
        jsonerror(ctx, boundedquoted(j), " is not a symbol of enum ", s.name)
    reserve!(ctx.budget, enumvaluebytes())
    v = EnumValue(s, Int32(budgetedgetindex(s.symbolindex, j, ctx.budget)), Val(:unchecked))
    allocated!(ctx.budget, enumvaluebytes())
    return v
end

function jsonkind(s::ArraySchema, j, ctx, depth)
    j isa JSONArray || jsonerror(ctx, "expected an array, got ", describejson(j))
    checkdepth(ctx.budget, depth)
    E = elementtype(s.items)
    n = length(j)
    reserve!(ctx.budget, vectorbytes(E, n))
    out = Vector{E}(undef, n)
    allocated!(ctx.budget, vectorbytes(E, n))
    for i in 1:n
        out[i] = jsontovalue(s.items, j[i], ctx, depth + 1)
    end
    return out
end

function jsonkind(s::MapSchema, j, ctx, depth)
    j isa JSONObject || jsonerror(ctx, "expected an object, got ", describejson(j))
    checkdepth(ctx.budget, depth)
    V = elementtype(s.values)
    ks = j.order.data
    n = length(ks)
    reserve!(ctx.budget, vectorbytes(String, n) + vectorbytes(V, n))   # buildmap charges the struct and permutation
    keys = Vector{String}(undef, n)
    vals = Vector{V}(undef, n)
    allocated!(ctx.budget, vectorbytes(String, n) + vectorbytes(V, n))
    for i in 1:n
        k = ks[i]
        isstrictutf8(k) ||
            jsonerror(ctx, "map key is not valid UTF-8 (lone surrogate escape)")
        if ctx.copystrings
            checkvaluebytes(ctx.budget, sizeof(k))
            keys[i] = ownedstringcopy(k, ctx.budget)
        else
            keys[i] = k
        end
        vals[i] = jsontovalue(s.values,
            budgetedgetindex(j.members, k, ctx.budget), ctx, depth + 1)
    end
    return buildmap(V, keys, vals, ctx.budget)
end

function jsonkind(s::RecordSchema, j, ctx, depth)
    j isa JSONObject ||
        jsonerror(ctx, "expected an object for record ", s.name,
                  ", got ", describejson(j))
    checkdepth(ctx.budget, depth)
    n = length(s.fields)
    if ctx.unknownerror
        for k in j.order
            budgetedhaskey(s.fieldindex, k, ctx.budget) ||
                jsonerror(ctx, "unknown member ", boundedquoted(k),
                          " of record ", s.name)
        end
    end
    reserve!(ctx.budget, recordbytes(n))
    vals = Vector{Any}(undef, n)
    allocated!(ctx.budget, vectorbytes(Any, n))        # the Record shell settles at construction
    for (i, f) in enumerate(s.fields)
        b = boxcharge(juliatype(f.schema))
        b > 0 && reserve!(ctx.budget, b)                                # before the assignment can box
        if budgetedhaskey(j.members, f.name, ctx.budget)
            vals[i] = jsontovalue(f.schema,
                budgetedgetindex(j.members, f.name, ctx.budget), ctx, depth + 1)
        elseif ctx.bareunion && f.default isa DefaultValue
            default = f.default
            vals[i] = jsontovalue(f.schema, default.json, ctx, depth + 1,
                                  default.branch)                            # recursive default rule
        else
            jsonerror(ctx, "missing field ", boundedquoted(f.name),
                      " of record ", s.name)
        end
        b > 0 && allocated!(ctx.budget, b)
    end
    r = Record(s, vals, Val(:unchecked))
    allocated!(ctx.budget, recordbytes(n) - vectorbytes(Any, n))
    return r
end

# A `UnionValue` wrapper charged and settled under the JSON decode budget (shell plus the box an
# isbits payload takes on assignment into the wrapper's Any field).
function chargedunion(i::Int, v, ctx)
    box = isbits(v) ? boxbytes(typeof(v)) : 0
    reserve!(ctx.budget, unionvaluebytes() + box)
    u = UnionValue(i, v)
    allocated!(ctx.budget, unionvaluebytes() + box)
    return u
end

function jsonkind(s::UnionSchema, j, ctx, depth)
    nb = nullablebranch(s)
    if ctx.branches !== nothing
        position = ctx.budget.default_position
        position <= length(ctx.branches) ||
            jsonerror(ctx, "resolved default branch tape is exhausted")
        i = ctx.branches[position]
        1 <= i <= length(s.branches) ||
            jsonerror(ctx, "resolved default branch tape has an out-of-range entry")
        ctx.budget.default_position = position + 1
        v = jsontovalue(s.branches[i], j, ctx, depth)
        return nb != 0 ? v : chargedunion(i, v, ctx)
    end
    if ctx.bareunion
        for (i, b) in enumerate(s.branches)
            checkpoint = budgetcheckpoint(ctx.budget)
            v = try
                countvalues!(ctx.budget)
                jsonkind(b, j, ctx, depth)
            catch e
                e isa DataError || rethrow()
                rollbackreservations!(ctx.budget, checkpoint)
                continue
            end
            return nb != 0 ? v : chargedunion(i, v, ctx)
        end
        jsonerror(ctx, "no union branch accepts the default ", describejson(j))
    end
    if j === nothing
        i = findfirst(b -> b isa NullSchema, s.branches)
        i === nothing && jsonerror(ctx, "union has no null branch")
        countvalues!(ctx.budget)
        return nb != 0 ? missing : chargedunion(i, missing, ctx)
    end
    (j isa JSONObject && length(j) == 1) ||
        jsonerror(ctx, "expected null or a one-member object for a union, got ",
                  describejson(j))
    label = j.order[1]
    i = branchbylabel(s, label, ctx.budget)
    i == 0 && jsonerror(ctx, boundedquoted(label),
                        " names no branch of the union")
    i == -1 && jsonerror(ctx, "union label ", boundedquoted(label),
                         " is ambiguous (a named type and a kind share it)")
    v = jsontovalue(s.branches[i],
        budgetedgetindex(j.members, label, ctx.budget), ctx, depth)
    return nb != 0 ? v : chargedunion(i, v, ctx)
end

function describejson(j)
    return j === nothing ? "null" : j isa Bool ? "a boolean" : j isa Int64 ? "an integer" : j isa Union{JSONNumber,Float64} ? "a number" :
           j isa String ? "a string" : j isa JSONArray ? "an array" : "an object"
end

function bytesfromjson(j, size::Int, ctx::JSONContext)
    j isa String || jsonerror(ctx, "expected a byte string, got ", describejson(j))
    isbytestring(j) || jsonerror(ctx, "byte string has code points above U+00FF")
    n = length(j)
    size < 0 || n == size ||
        jsonerror(ctx, "fixed of size ", size, " given ", n, " bytes")
    checkvaluebytes(ctx.budget, n)
    reserve!(ctx.budget, bytesbytes(n))
    out = Vector{UInt8}(undef, n)
    allocated!(ctx.budget, bytesbytes(n))
    for (i, c) in enumerate(j)
        out[i] = UInt8(c)
    end
    return out
end

function decimalfromvector(bytes::Vector{UInt8}, l::DecimalLogical, ctx::JSONContext)
    isempty(bytes) && jsonerror(ctx, "empty decimal payload")
    d = Decoder(bytes, ctx.budget)
    return decimalfrombytes(d, DecimalPlan(0, l.precision, l.scale, l.precision > 38), 1, length(bytes))
end

function uuidfrombytes(bytes::Vector{UInt8})
    v = UInt128(0)
    for b in bytes
        v = (v << 8) | UInt128(b)
    end
    return UUID(v)
end

function le32(bytes::Vector{UInt8}, i::Int)
    return UInt32(bytes[i]) | (UInt32(bytes[i + 1]) << 8) | (UInt32(bytes[i + 2]) << 16) | (UInt32(bytes[i + 3]) << 24)
end

function printkind(out::JSONOut, s::BytesSchema, v::DataDecimals.AbstractDecimal, depth, budget)
    return printkind(out, s, _decimalinput(v), depth, budget)
end
