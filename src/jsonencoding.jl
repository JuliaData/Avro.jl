# JSON encoding (plan §4.11). `tojson` validates through the binary write plan (identical validation and
# union branch recovery to `encode`), decodes the bytes back into the closed generic value set and prints
# that with Java-compatible labels; `fromjson` parses with Avro's own bounded JSON reader (never JSON.jl)
# and converts by the §4.11 rule table. Both are bounded by `max_datum_bytes`, `max_total_values`,
# `max_json_depth` and the operation budget.

struct JSONOut
    io::IOBuffer
    maxbytes::Int
    pretty::Bool
end

function checkoutput!(out::JSONOut)
    n = out.io.size
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
    bytes = encode(s, x; limits=limits)
    return withbudget(limits; direction=:encode) do budget
        addinput!(budget, length(bytes))
        d = Decoder(bytes, budget)
        v = decode(readplan(s; budget=budget), d)
        out = JSONOut(IOBuffer(), limits.max_datum_bytes, pretty)
        printvalue(out, s, v, 0, budget)
        checkoutput!(out)
        return String(take!(out.io))
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
    return printfloat(out.io, v)
end

function printkind(out::JSONOut, ::StringSchema, v::String, depth, budget)
    return escapejson(out.io, v)
end

function printkind(out::JSONOut, ::StringSchema, v::UUID, depth, budget)
    return escapejson(out.io, string(v))
end

function printkind(out::JSONOut, ::EnumSchema, v::EnumValue, depth, budget)
    return escapejson(out.io, String(v))
end

function printkind(out::JSONOut, ::BytesSchema, v::Vector{UInt8}, depth, budget)
    return printbytestring(out.io, v)
end

function printkind(out::JSONOut, ::BytesSchema, v::Union{Decimal,WideDecimal}, depth, budget)
    return printbytestring(out.io, twoscomplement(BigInt(v.unscaled)))
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
    v isa Fixed && return printbytestring(out.io, v.bytes)
    v isa UUID && return printbytestring(out.io, uuidbytes(v))
    v isa Duration && return printbytestring(out.io, durationbytes(v))
    return printbytestring(out.io, padtwoscomplement(twoscomplement(BigInt(v.unscaled)), s.size))
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
        v === missing && return print(out.io, "null")
        return printmember(out, s, 3 - nb, v, depth, budget)
    end
    u = v::UnionValue
    branch = s.branches[u.index]
    branch isa NullSchema && return print(out.io, "null")
    return printmember(out, s, u.index, u.value, depth, budget)
end

function printmember(out::JSONOut, s::UnionSchema, i::Int, v, depth, budget)
    branch = s.branches[i]
    label = unionlabel(branch)
    ambiguouslabel(s, label) && throw(EncodeError("union label \"$label\" is ambiguous (a named type and a kind share it)", "", s))
    io = out.io
    print(io, '{')
    indent(io, out.pretty, depth + 1)
    escapejson(io, label)
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
    return string(kind(s))
end

function ambiguouslabel(s::UnionSchema, label::String)
    return count(b -> unionlabel(b) == label, s.branches) > 1
end

function branchbylabel(s::UnionSchema, label::String)
    found = 0
    for (i, b) in enumerate(s.branches)
        unionlabel(b) == label || continue
        found == 0 || return -1
        found = i
    end
    return found
end

function printfloat(io::IO, v::AbstractFloat)
    isfinite(v) || return print(io, isnan(v) ? "\"NaN\"" : (v > 0 ? "\"Infinity\"" : "\"-Infinity\""))
    v isa Float32 && return print(io, replace(string(v), 'f' => 'e'))
    return print(io, Float64(v))
end

# Bytes as a JSON string of code points U+0000–U+00FF (Java's form).
function printbytestring(io::IO, bytes::AbstractVector{UInt8})
    print(io, '"')
    for b in bytes
        if b == UInt8('"')
            print(io, "\\\"")
        elseif b == UInt8('\\')
            print(io, "\\\\")
        elseif b < 0x20
            if b == 0x08; print(io, "\\b") elseif b == 0x0C; print(io, "\\f") elseif b == 0x0A; print(io, "\\n")
            elseif b == 0x0D; print(io, "\\r") elseif b == 0x09; print(io, "\\t")
            else print(io, "\\u", string(b; base=16, pad=4)) end
        elseif b < 0x80
            Base.write(io, b)
        else
            Base.write(io, 0xC0 | (b >> 6), 0x80 | (b & 0x3F))
        end
    end
    print(io, '"')
    return nothing
end

function padtwoscomplement(bytes::Vector{UInt8}, size::Int)
    length(bytes) <= size || throw(EncodeError("decimal does not fit the fixed size $size", "", nothing))
    pad = bytes[1] >= 0x80 ? 0xff : 0x00
    return vcat(fill(pad, size - length(bytes)), bytes)
end

function durationbytes(x::Duration)
    out = Vector{UInt8}(undef, 12)
    for (k, v) in enumerate((x.months, x.days, x.millis))
        for i in 0:3
            out[4 * (k - 1) + i + 1] = UInt8((v >> (8 * i)) & 0xff)
        end
    end
    return out
end

# ---- fromjson ---------------------------------------------------------------------------------------

struct JSONContext
    budget::Budget
    strict::Bool
    unknownerror::Bool
    bareunion::Bool        # defaults: unions are bare values matched in branch order (plan §4.2)
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
    return semanticvalue(T, v, admission(names))
end

"""
    jsonvalue(schema, json, budget) -> value

Convert a frozen JSON tree parsed in *default* context (bare unions, the recursive record rule) into the
generic value model — used for field defaults under resolution.
"""
function jsonvalue(s::Schema, j, budget::Budget)
    return jsontovalue(s, j, JSONContext(budget, true, false, true), 1)
end

function jsonerror(msg::AbstractString)
    throw(DataError(msg, 0))
end

function jsontovalue(s::Schema, j, ctx::JSONContext, depth::Int)
    countvalues!(ctx.budget)
    return jsonkind(s, j, ctx, depth)
end

function jsonkind(::NullSchema, j, ctx, depth)
    return j === nothing ? missing : jsonerror("expected null, got $(describejson(j))")
end

function jsonkind(::BooleanSchema, j, ctx, depth)
    return j isa Bool ? j : jsonerror("expected a boolean, got $(describejson(j))")
end

function jsonkind(s::IntSchema, j, ctx, depth)
    j isa Int64 || jsonerror("expected an integer, got $(describejson(j))")
    typemin(Int32) <= j <= typemax(Int32) || jsonerror("integer $j does not fit int")
    l = s.logical
    l isa DateLogical && return DATE_EPOCH + Day(j)
    if l isa TimeMillis
        0 <= j < 86_400_000 || jsonerror("time-millis $j is outside one day")
        return Time(Nanosecond(j * 1_000_000))
    end
    return Int32(j)
end

function jsonkind(s::LongSchema, j, ctx, depth)
    j isa Int64 || jsonerror("expected an integer, got $(describejson(j))")
    l = s.logical
    if l isa TimeMicros
        0 <= j < 86_400_000_000 || jsonerror("time-micros $j is outside one day")
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
    return jsonfloat(Float32, j)
end

function jsonkind(::DoubleSchema, j, ctx, depth)
    return jsonfloat(Float64, j)
end

function jsonfloat(::Type{T}, j) where {T}
    j isa Int64 && return T(j)
    j isa Float64 && return T(j)                 # lenient non-finite tokens
    j isa JSONNumber && return parsefloat(T, j.text)
    if j isa String
        j == "NaN" && return T(NaN)
        j == "Infinity" && return T(Inf)
        j == "-Infinity" && return T(-Inf)
    end
    return jsonerror("expected a number (or \"NaN\"/\"Infinity\"/\"-Infinity\"), got $(describejson(j))")
end

function jsonkind(s::BytesSchema, j, ctx, depth)
    bytes = bytesfromjson(j, -1, ctx)
    l = s.logical
    l isa DecimalLogical && return decimalfromvector(bytes, l, ctx)
    return bytes
end

function jsonkind(s::FixedSchema, j, ctx, depth)
    bytes = bytesfromjson(j, s.size, ctx)
    l = s.logical
    l isa DecimalLogical && return decimalfromvector(bytes, l, ctx)
    l isa UUIDLogical && return uuidfrombytes(bytes)
    l isa DurationLogical && return Duration(le32(bytes, 1), le32(bytes, 5), le32(bytes, 9))
    return Fixed(s, bytes, Val(:unchecked))
end

function jsonkind(s::StringSchema, j, ctx, depth)
    j isa String || jsonerror("expected a string, got $(describejson(j))")
    isstrictutf8(j) || jsonerror("string is not valid UTF-8 (lone surrogate escape)")
    if s.logical isa UUIDLogical
        u = tryparseuuid(j)
        u === nothing && jsonerror("not an RFC 4122 uuid string: $(repr(j))")
        return u
    end
    reserve!(ctx.budget, stringbytes(sizeof(j)))
    allocated!(ctx.budget, stringbytes(sizeof(j)))     # the reader's string is already resident; it transfers here
    return j
end

function jsonkind(s::EnumSchema, j, ctx, depth)
    j isa String || jsonerror("expected an enum symbol string, got $(describejson(j))")
    haskey(s.symbolindex, j) || jsonerror("\"$j\" is not a symbol of enum $(fullname(s))")
    reserve!(ctx.budget, enumvaluebytes())
    v = EnumValue(s, Int32(s.symbolindex[j]), Val(:unchecked))
    allocated!(ctx.budget, enumvaluebytes())
    return v
end

function jsonkind(s::ArraySchema, j, ctx, depth)
    j isa JSONArray || jsonerror("expected an array, got $(describejson(j))")
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
    j isa JSONObject || jsonerror("expected an object, got $(describejson(j))")
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
        isstrictutf8(k) || jsonerror("map key is not valid UTF-8 (lone surrogate escape)")
        keys[i] = k
        vals[i] = jsontovalue(s.values, j[k], ctx, depth + 1)
    end
    return buildmap(V, keys, vals, ctx.budget)
end

function jsonkind(s::RecordSchema, j, ctx, depth)
    j isa JSONObject || jsonerror("expected an object for record $(fullname(s)), got $(describejson(j))")
    checkdepth(ctx.budget, depth)
    n = length(s.fields)
    if ctx.unknownerror
        for k in j.order
            haskey(s.fieldindex, k) || jsonerror("unknown member \"$k\" of record $(fullname(s))")
        end
    end
    reserve!(ctx.budget, recordbytes(n))
    vals = Vector{Any}(undef, n)
    allocated!(ctx.budget, vectorbytes(Any, n))        # the Record shell settles at construction
    for (i, f) in enumerate(s.fields)
        b = boxcharge(juliatype(f.schema))
        b > 0 && reserve!(ctx.budget, b)                                # before the assignment can box
        if haskey(j, f.name)
            vals[i] = jsontovalue(f.schema, j[f.name], ctx, depth + 1)
        elseif ctx.bareunion && f.default isa DefaultValue
            vals[i] = jsontovalue(f.schema, f.default.json, ctx, depth + 1)   # recursive default rule
        else
            jsonerror("missing field \"$(f.name)\" of record $(fullname(s))")
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
    if ctx.bareunion
        for (i, b) in enumerate(s.branches)
            v = try
                jsonkind(b, j, ctx, depth)
            catch e
                e isa DataError || rethrow()
                continue
            end
            return nb != 0 ? v : chargedunion(i, v, ctx)
        end
        jsonerror("no union branch accepts the default $(describejson(j))")
    end
    if j === nothing
        i = findfirst(b -> b isa NullSchema, s.branches)
        i === nothing && jsonerror("union has no null branch")
        return nb != 0 ? missing : chargedunion(i, missing, ctx)
    end
    (j isa JSONObject && length(j) == 1) || jsonerror("expected null or a one-member object for a union, got $(describejson(j))")
    label = j.order[1]
    i = branchbylabel(s, label)
    i == 0 && jsonerror("\"$label\" names no branch of the union")
    i == -1 && jsonerror("union label \"$label\" is ambiguous (a named type and a kind share it)")
    checkdepth(ctx.budget, depth)
    v = jsontovalue(s.branches[i], j[label], ctx, depth + 1)
    return nb != 0 ? v : chargedunion(i, v, ctx)
end

function describejson(j)
    return j === nothing ? "null" : j isa Bool ? "a boolean" : j isa Int64 ? "an integer" : j isa Union{JSONNumber,Float64} ? "a number" :
           j isa String ? "a string" : j isa JSONArray ? "an array" : "an object"
end

function bytesfromjson(j, size::Int, ctx::JSONContext)
    j isa String || jsonerror("expected a byte string, got $(describejson(j))")
    isbytestring(j) || jsonerror("byte string has code points above U+00FF")
    n = length(j)
    size < 0 || n == size || jsonerror("fixed of size $size given $n bytes")
    reserve!(ctx.budget, bytesbytes(n))
    out = Vector{UInt8}(undef, n)
    allocated!(ctx.budget, bytesbytes(n))
    for (i, c) in enumerate(j)
        out[i] = UInt8(c)
    end
    return out
end

function decimalfromvector(bytes::Vector{UInt8}, l::DecimalLogical, ctx::JSONContext)
    isempty(bytes) && jsonerror("empty decimal payload")
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
