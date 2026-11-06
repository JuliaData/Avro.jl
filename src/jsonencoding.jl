# JSON encoding. `tojson` encodes through the binary write plan (the validation and union branch
# selection of `encode`) and prints the encoded datum with Java-compatible labels; `fromjson` parses with
# Avro's own JSON reader (never JSON.jl) and converts by the JSON encoding rules into the generic values.

"""
    Avro.tojson(schema, x; pretty=false) -> String

The Avro JSON encoding of `x` under `schema` (Java-compatible: unions as one-member objects keyed by
the branch's fullname or type name (the null branch as `null`), bytes/fixed as strings of code points U+0000–U+00FF, non-finite
floats as the strings `"NaN"`, `"Infinity"`, `"-Infinity"`; map entries in key order). Validation and
union branch selection are those of `Avro.encode`.
"""
function tojson(s::Schema, x; pretty::Bool=false, limits::Limits=Limits())
    graphinfo(s).repaired_names &&
        throw(ArgumentError("tojson rejects a schema parsed with allow_invalid_names=true"))
    e = Encoder(limits)
    encodedatum!(writeplan(s), e, x, s)
    io = IOBuffer()
    printdatum(io, s, Decoder(e.buf, uncapped(limits); stop=e.pos), pretty, 0)
    return String(take!(io))
end

# The printer reads the datum `encodedatum!` just wrote: logical types print as their underlying type
# and a union prints the label of the branch the encoder selected.
function printdatum(io::IO, ::NullSchema, d::Decoder, pretty::Bool, level::Int)
    return print(io, "null")
end

function printdatum(io::IO, ::BooleanSchema, d::Decoder, pretty::Bool, level::Int)
    return print(io, readbool(d))
end

function printdatum(io::IO, ::IntSchema, d::Decoder, pretty::Bool, level::Int)
    return print(io, readint(d))
end

function printdatum(io::IO, ::LongSchema, d::Decoder, pretty::Bool, level::Int)
    return print(io, readlong(d))
end

function printdatum(io::IO, ::FloatSchema, d::Decoder, pretty::Bool, level::Int)
    return printfloat(io, readfloat(d))
end

function printdatum(io::IO, ::DoubleSchema, d::Decoder, pretty::Bool, level::Int)
    return printfloat(io, readdouble(d))
end

function printdatum(io::IO, ::BytesSchema, d::Decoder, pretty::Bool, level::Int)
    n = readlen(d)
    printbytes(io, d.buf, d.pos, n)
    d.pos += n
    return nothing
end

function printdatum(io::IO, s::FixedSchema, d::Decoder, pretty::Bool, level::Int)
    p = d.pos
    skipfixed(d, s.size)
    return printbytes(io, d.buf, p, s.size)
end

function printdatum(io::IO, s::StringSchema, d::Decoder, pretty::Bool, level::Int)
    text = readstring(d)
    # uuid text is encoded as given; print the canonical lowercase form a decoded UUID has
    return escapejson(io, s.logical isa UUIDLogical ? lowercase(text) : text)
end

function printdatum(io::IO, s::EnumSchema, d::Decoder, pretty::Bool, level::Int)
    return escapejson(io, s.symbols[readindex(d, length(s.symbols))])
end

function printdatum(io::IO, s::ArraySchema, d::Decoder, pretty::Bool, level::Int)
    print(io, '[')
    n = 0
    while true
        count, _ = readblockcount(d)
        count == 0 && break
        for _ in 1:count
            n > 0 && print(io, ',')
            n += 1
            indent(io, pretty, level + 1)
            printdatum(io, s.items, d, pretty, level + 1)
        end
    end
    n > 0 && indent(io, pretty, level)
    return print(io, ']')
end

function printdatum(io::IO, s::MapSchema, d::Decoder, pretty::Bool, level::Int)
    entries = Pair{String,String}[]
    while true
        count, _ = readblockcount(d)
        count == 0 && break
        for _ in 1:count
            key = readstring(d)
            value = IOBuffer()
            printdatum(value, s.values, d, pretty, level + 1)
            push!(entries, key => String(take!(value)))
        end
    end
    sort!(entries; by=first)
    print(io, '{')
    for (i, (key, value)) in enumerate(entries)
        i > 1 && print(io, ',')
        indent(io, pretty, level + 1)
        escapejson(io, key)
        print(io, pretty ? ": " : ":", value)
    end
    isempty(entries) || indent(io, pretty, level)
    return print(io, '}')
end

function printdatum(io::IO, s::RecordSchema, d::Decoder, pretty::Bool, level::Int)
    print(io, '{')
    for (i, f) in enumerate(s.fields)
        i > 1 && print(io, ',')
        indent(io, pretty, level + 1)
        escapejson(io, f.name)
        print(io, pretty ? ": " : ":")
        printdatum(io, f.schema, d, pretty, level + 1)
    end
    isempty(s.fields) || indent(io, pretty, level)
    return print(io, '}')
end

function printdatum(io::IO, s::UnionSchema, d::Decoder, pretty::Bool, level::Int)
    i = readindex(d, length(s.branches))
    branch = s.branches[i]
    branch isa NullSchema && return print(io, "null")
    ambiguouslabel(s, i) &&
        throw(EncodeError("union label is ambiguous (a named type and a kind share it)", "\$", s))
    print(io, '{')
    indent(io, pretty, level + 1)
    escapejson(io, branchidentitylabel(branch))
    print(io, pretty ? ": " : ":")
    printdatum(io, branch, d, pretty, level + 1)
    indent(io, pretty, level)
    return print(io, '}')
end

function ambiguouslabel(s::UnionSchema, i::Int)
    label = branchidentitylabel(s.branches[i])
    return count(b -> branchidentitylabel(b) == label, s.branches) > 1
end

# The branch of `s` labelled `label`: 0 when none is, -1 when several are.
function branchbylabel(s::UnionSchema, label::String)
    found = 0
    for (i, b) in enumerate(s.branches)
        branchidentitylabel(b) == label || continue
        found == 0 || return -1
        found = i
    end
    return found
end

function printfloat(io::IO, v::AbstractFloat)
    isnan(v) && return print(io, "\"NaN\"")
    isinf(v) && return print(io, v > 0 ? "\"Infinity\"" : "\"-Infinity\"")
    return print(io, Base.Ryu.writeshortest(v))
end

# Bytes as a JSON string of code points U+0000–U+00FF (Java's form).
function printbytes(io::IO, buf::AbstractVector{UInt8}, from::Int, n::Int)
    return escapejson(io, String(Char.(view(buf, from:from + n - 1))))
end

# ---- fromjson ---------------------------------------------------------------------------------------

"""
    Avro.fromjson(schema, json; strict=true, unknown=:ignore, limits=Limits())
    Avro.fromjson(schema, json, T; kw...)

Decode the Avro JSON encoding of one datum (`json` is a string, bytes or an `IO`) into the generic value
model, or into `T` through the semantic StructUtils route. `strict=false` additionally accepts the bare
tokens `NaN`/`Infinity`/`-Infinity`; `unknown=:error` rejects unknown record members (ignored by
default). The document is bounded by `limits.max_bytes` and `limits.max_depth`.
"""
function fromjson(s::Schema, src::Union{AbstractString,AbstractVector{UInt8},IO}; strict::Bool=true,
                  unknown::Symbol=:ignore, limits::Limits=Limits())
    graphinfo(s).repaired_names &&
        throw(ArgumentError("fromjson rejects a schema parsed with allow_invalid_names=true"))
    unknown in (:ignore, :error) || throw(ArgumentError("unknown must be :ignore or :error"))
    bytes = sourcebytes(src, limits.max_bytes)
    j = parsejson(bytes; limits=limits, errfn=(msg, pos) -> throw(DataError(msg, pos)), lenient=!strict)
    return jsondatum(s, j, unknown === :error)
end

function fromjson(s::Schema, src, ::Type{T}; kw...) where {T}
    v = fromjson(s, src; kw...)
    (T === Any || v isa T) && return v
    return semanticvalue(T, v, s)
end

function jsonfail(parts...)
    throw(DataError(string(parts...), 0))
end

# A JSON-quoted string for an error message, truncated when long.
function describejson(j)
    j === nothing && return "null"
    j isa Bool && return string(j)
    j isa Int64 && return string("the integer ", j)
    j isa JSONNumber && return string("the number ", quoted(j.text))
    j isa Float64 && return string("the number ", j)
    j isa String && return quoted(j)
    return j isa Vector{Any} ? "an array" : "an object"
end

# A leaf takes the same JSON values as a schema default; `unknown` says whether a record member that
# names no field is an error.
function jsondatum(s::Union{PrimitiveSchema,FixedSchema,EnumSchema}, j, unknown::Bool)
    v = defaultvalue(s, j)
    v === nothing && jsonfail("expected ", schemalabel(s), ", got ", describejson(j))
    return v
end

function jsondatum(s::ArraySchema, j, unknown::Bool)
    j isa Vector{Any} || jsonfail("expected an array, got ", describejson(j))
    out = Vector{elementtype(s.items)}(undef, length(j))
    for i in eachindex(j)
        out[i] = jsondatum(s.items, j[i], unknown)
    end
    return out
end

function jsondatum(s::MapSchema, j, unknown::Bool)
    j isa JSON.Object || jsonfail("expected an object, got ", describejson(j))
    out = Dict{String,elementtype(s.values)}()
    for (k, v) in j
        isstrictutf8(k) || jsonfail("map key is not valid UTF-8 (lone surrogate escape)")
        out[k] = jsondatum(s.values, v, unknown)
    end
    return out
end

function jsondatum(s::RecordSchema, j, unknown::Bool)
    j isa JSON.Object || jsonfail("expected an object for record ", fullname(s), ", got ", describejson(j))
    if unknown
        names = Set(f.name for f in s.fields)
        for k in keys(j)
            k in names || jsonfail("unknown member ", quoted(k), " of record ", fullname(s))
        end
    end
    members = memberlookup(j)
    values = Vector{Any}(undef, length(s.fields))
    for (i, f) in enumerate(s.fields)
        member = get(() -> jsonfail("missing field ", quoted(f.name), " of record ", fullname(s)), members, f.name)
        values[i] = jsondatum(f.schema, member, unknown)
    end
    return Record(s, values, Val(:unchecked))
end

function jsondatum(s::UnionSchema, j, unknown::Bool)
    if j === nothing
        any(b -> b isa NullSchema, s.branches) || jsonfail("union has no null branch")
        return missing
    end
    (j isa JSON.Object && length(j) == 1) ||
        jsonfail("expected null or a one-member object for a union, got ", describejson(j))
    label, value = first(j)
    i = branchbylabel(s, label)
    i == 0 && jsonfail(quoted(label), " names no branch of the union")
    i == -1 && jsonfail("union label ", quoted(label), " is ambiguous (a named type and a kind share it)")
    return jsondatum(s.branches[i], value, unknown)
end
