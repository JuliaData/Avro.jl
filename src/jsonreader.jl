# Avro-owned JSON reader (plan §4.2, decision 51): an allocation-free lexical pre-scan (size, depth,
# strict UTF-8, escape syntax, number grammar, literals, bracket balance, trailing content) followed by a
# recursive-descent parser that builds frozen JSON trees with raw number tokens and WTF-8 strings.
# JSON.jl is never used for parsing.

struct JSONSyntaxError <: Exception
    msg::String
    pos::Int
end

Base.showerror(io::IO, e::JSONSyntaxError) = print(io, "JSON syntax error at byte ", e.pos, ": ", e.msg)

const WS = (UInt8(' '), UInt8('\t'), UInt8('\n'), UInt8('\r'))

isws(b::UInt8) = b == UInt8(' ') || b == UInt8('\t') || b == UInt8('\n') || b == UInt8('\r')
isdigit8(b::UInt8) = UInt8('0') <= b <= UInt8('9')
ishex8(b::UInt8) = isdigit8(b) || (UInt8('a') <= b <= UInt8('f')) || (UInt8('A') <= b <= UInt8('F'))

"""
    utf8_valid_length(buf, i, n) -> Int

Length (1–4) of the strictly valid UTF-8 sequence starting at `buf[i]`, or 0 when it is malformed
(overlong, surrogate code point, above U+10FFFF, truncated, bad continuation).
"""
function utf8_valid_length(buf::AbstractVector{UInt8}, i::Int, n::Int)
    b0 = buf[i]
    b0 < 0x80 && return 1
    if 0xC2 <= b0 <= 0xDF
        i + 1 <= n || return 0
        return (buf[i + 1] & 0xC0) == 0x80 ? 2 : 0
    elseif 0xE0 <= b0 <= 0xEF
        i + 2 <= n || return 0
        b1 = buf[i + 1]; b2 = buf[i + 2]
        (b1 & 0xC0) == 0x80 && (b2 & 0xC0) == 0x80 || return 0
        b0 == 0xE0 && b1 < 0xA0 && return 0          # overlong
        b0 == 0xED && b1 >= 0xA0 && return 0         # surrogate code point
        return 3
    elseif 0xF0 <= b0 <= 0xF4
        i + 3 <= n || return 0
        b1 = buf[i + 1]; b2 = buf[i + 2]; b3 = buf[i + 3]
        (b1 & 0xC0) == 0x80 && (b2 & 0xC0) == 0x80 && (b3 & 0xC0) == 0x80 || return 0
        b0 == 0xF0 && b1 < 0x90 && return 0          # overlong
        b0 == 0xF4 && b1 >= 0x90 && return 0         # above U+10FFFF
        return 4
    end
    return 0
end

"""
    isstrictutf8(s) -> Bool

Strict UTF-8 validity (no overlongs, no surrogates, nothing above U+10FFFF) of a string or byte vector.
"""
function isstrictutf8(buf::AbstractVector{UInt8})
    i = 1; n = length(buf)
    while i <= n
        len = utf8_valid_length(buf, i, n)
        len == 0 && return false
        i += len
    end
    return true
end

isstrictutf8(s::AbstractString) = isstrictutf8(codeunits(s))

# ---- lexical pre-scan -------------------------------------------------------------------------

"""
    prescan(buf, maxbytes, maxdepth, errfn) -> depth

Validate the byte buffer as one JSON document without allocating: size ≤ `maxbytes`, nesting depth ≤
`maxdepth`, strict UTF-8 everywhere, escape syntax, number grammar, exact literals, bracket balance, no
BOM, no raw control characters in strings, no trailing content. `errfn(msg, pos)` must throw the
appropriate error (`SchemaError`, `DataError`, …). Returns the maximum nesting depth.
"""
function prescan(buf::AbstractVector{UInt8}, maxbytes::Int, maxdepth::Int, errfn;
                 limitfn=(limit, observed, value) -> errfn("$limit exceeded ($observed > $value)", 0),
                 bytelimit::Symbol=:max_bytes, depthlimit::Symbol=:max_depth, lenient::Bool=false)
    n = length(buf)
    n <= maxbytes || limitfn(bytelimit, n, maxbytes)
    n >= 3 && buf[1] == 0xEF && buf[2] == 0xBB && buf[3] == 0xBF && errfn("a byte-order mark is not allowed", 1)
    i = 1
    depth = 0
    maxseen = 0
    stack = UInt128(0)        # bracket kinds for depths ≤ 128; deeper levels are checked by the parser
    nvalues = 0
    while i <= n
        b = buf[i]
        if isws(b)
            i += 1
        elseif b == UInt8('{') || b == UInt8('[')
            depth += 1
            depth <= maxdepth || limitfn(depthlimit, depth, maxdepth)
            maxseen = max(maxseen, depth)
            depth <= 128 && (stack = b == UInt8('{') ? (stack | (UInt128(1) << (depth - 1))) : (stack & ~(UInt128(1) << (depth - 1))))
            nvalues += 1
            i += 1
        elseif b == UInt8('}') || b == UInt8(']')
            depth >= 1 || errfn("unexpected closing bracket", i)
            if depth <= 128
                isobj = (stack >> (depth - 1)) & 1 == 1
                (b == UInt8('}')) == isobj || errfn("mismatched closing bracket", i)
            end
            depth -= 1
            i += 1
        elseif b == UInt8('"')
            i = scanstring(buf, i, n, errfn)
            nvalues += 1
        elseif b == UInt8(',') || b == UInt8(':')
            depth >= 1 || errfn("unexpected '$(Char(b))' outside a container", i)
            i += 1
        elseif lenient && (b == UInt8('N') || b == UInt8('I') || (b == UInt8('-') && i < n && buf[i + 1] == UInt8('I')))
            i = scanliteral(buf, i, n, b == UInt8('N') ? "NaN" : (b == UInt8('I') ? "Infinity" : "-Infinity"), errfn)
            nvalues += 1
        elseif b == UInt8('-') || isdigit8(b)
            i = scannumber(buf, i, n, errfn)
            nvalues += 1
        elseif b == UInt8('t')
            i = scanliteral(buf, i, n, "true", errfn)
            nvalues += 1
        elseif b == UInt8('f')
            i = scanliteral(buf, i, n, "false", errfn)
            nvalues += 1
        elseif b == UInt8('n')
            i = scanliteral(buf, i, n, "null", errfn)
            nvalues += 1
        elseif b >= 0x80
            len = utf8_valid_length(buf, i, n)
            len == 0 && errfn("invalid UTF-8 sequence", i)
            errfn("unexpected non-ASCII character outside a string", i)
        else
            errfn("unexpected byte 0x$(string(b; base=16, pad=2))", i)
        end
    end
    depth == 0 || errfn("unterminated container (missing closing bracket)", n + 1)
    nvalues >= 1 || errfn("empty JSON document", 1)
    return maxseen
end

function scanliteral(buf, i, n, lit::String, errfn)
    cu = codeunits(lit)
    i + length(cu) - 1 <= n || errfn("malformed literal", i)
    for k in 1:length(cu)
        buf[i + k - 1] == cu[k] || errfn("malformed literal", i)
    end
    j = i + length(cu)
    j <= n && (isnamechar(buf[j]) || buf[j] >= 0x80) && errfn("malformed literal", i)
    return j
end

function scanstring(buf, i, n, errfn)
    i += 1
    while true
        i <= n || errfn("unterminated string", n + 1)
        b = buf[i]
        if b == UInt8('"')
            return i + 1
        elseif b == UInt8('\\')
            i + 1 <= n || errfn("unterminated escape", i)
            e = buf[i + 1]
            if e == UInt8('u')
                i + 5 <= n || errfn("truncated \\u escape", i)
                for k in 2:5
                    ishex8(buf[i + k]) || errfn("invalid \\u escape", i)
                end
                i += 6
            elseif e in (UInt8('"'), UInt8('\\'), UInt8('/'), UInt8('b'), UInt8('f'), UInt8('n'), UInt8('r'), UInt8('t'))
                i += 2
            else
                errfn("invalid escape \\$(Char(e))", i)
            end
        elseif b < 0x20
            errfn("raw control character in string", i)
        elseif b >= 0x80
            len = utf8_valid_length(buf, i, n)
            len == 0 && errfn("invalid UTF-8 sequence in string", i)
            i += len
        else
            i += 1
        end
    end
end

"""
    scannumber(buf, i, n, errfn) -> nextpos

Validate a number token against the RFC 8259 grammar `-?(0|[1-9][0-9]*)(\\.[0-9]+)?([eE][+-]?[0-9]+)?`.
"""
function scannumber(buf, i, n, errfn)
    start = i
    buf[i] == UInt8('-') && (i += 1)
    i <= n || errfn("malformed number", start)
    if buf[i] == UInt8('0')
        i += 1
    elseif UInt8('1') <= buf[i] <= UInt8('9')
        i += 1
        while i <= n && isdigit8(buf[i])
            i += 1
        end
    else
        errfn("malformed number", start)
    end
    if i <= n && buf[i] == UInt8('.')
        i += 1
        (i <= n && isdigit8(buf[i])) || errfn("malformed number (fraction)", start)
        while i <= n && isdigit8(buf[i])
            i += 1
        end
    end
    if i <= n && (buf[i] == UInt8('e') || buf[i] == UInt8('E'))
        i += 1
        i <= n && (buf[i] == UInt8('+') || buf[i] == UInt8('-')) && (i += 1)
        (i <= n && isdigit8(buf[i])) || errfn("malformed number (exponent)", start)
        while i <= n && isdigit8(buf[i])
            i += 1
        end
    end
    i <= n && (isnamechar(buf[i]) || buf[i] == UInt8('.') || buf[i] >= 0x80) && errfn("malformed number", start)
    return i
end

# ---- number conversion ------------------------------------------------------------------------

"""
    parseinteger(token) -> Union{Nothing,Int64}

Checked `Int64` accumulation of an integer token; `nothing` on overflow or when the token has a
fraction or exponent.
"""
function parseinteger(tok::AbstractString)
    cu = codeunits(tok)
    isempty(cu) && return nothing
    neg = cu[1] == UInt8('-')
    i = neg ? 2 : 1
    i <= length(cu) || return nothing
    acc = Int64(0)
    while i <= length(cu)
        b = cu[i]
        isdigit8(b) || return nothing
        d = Int64(b - UInt8('0'))
        acc, o1 = Base.Checked.mul_with_overflow(acc, Int64(10))
        o1 && return nothing
        acc, o2 = neg ? Base.Checked.sub_with_overflow(acc, d) : Base.Checked.add_with_overflow(acc, d)
        o2 && return nothing
        i += 1
    end
    return acc
end

"""
    parsefloat(T, token) -> T

Correctly rounded, linear-time parse of a JSON number token as `Float32` or `Float64` (overflow gives
±Inf, underflow ±0.0 or a subnormal, `-0.0` preserved, as Java's `parseDouble`).
"""
function parsefloat(::Type{T}, tok::AbstractString) where {T<:Union{Float32,Float64}}
    v = tryparse(T, tok)
    v === nothing || return v
    # Julia's parser rejects out-of-range tokens; the pre-scan guarantees the grammar, so classify
    # overflow (±Inf) versus underflow (±0.0) from the effective decimal exponent.
    cu = codeunits(tok)
    (isempty(cu) || scannumber_ok(cu)) || throw(ArgumentError("not a float token: $tok"))
    neg = cu[1] == UInt8('-')
    i = neg ? 2 : 1
    intdigits = 0
    nonzero = false
    while i <= length(cu) && isdigit8(cu[i])
        cu[i] != UInt8('0') && (nonzero = true)
        nonzero && (intdigits += 1)
        i += 1
    end
    leadingfrac = 0
    if i <= length(cu) && cu[i] == UInt8('.')
        i += 1
        while i <= length(cu) && isdigit8(cu[i])
            if !nonzero
                if cu[i] == UInt8('0')
                    leadingfrac += 1
                else
                    nonzero = true
                end
            end
            i += 1
        end
    end
    nonzero || return neg ? -zero(T) : zero(T)
    expo = 0
    if i <= length(cu) && (cu[i] == UInt8('e') || cu[i] == UInt8('E'))
        i += 1
        eneg = false
        if i <= length(cu) && (cu[i] == UInt8('+') || cu[i] == UInt8('-'))
            eneg = cu[i] == UInt8('-')
            i += 1
        end
        while i <= length(cu) && isdigit8(cu[i])
            expo = min(expo * 10 + Int(cu[i] - UInt8('0')), 1_000_000)
            i += 1
        end
        eneg && (expo = -expo)
    end
    effective = intdigits > 0 ? expo + intdigits - 1 : expo - leadingfrac - 1
    effective >= 0 && return neg ? -T(Inf) : T(Inf)
    return neg ? -zero(T) : zero(T)
end

function scannumber_ok(cu)
    try
        scannumber(cu, 1, length(cu), (msg, pos) -> throw(ArgumentError(msg))) == length(cu) + 1 || return false
    catch
        return false
    end
    return true
end

# ---- WTF-8 string decoding ---------------------------------------------------------------------

hexval(b::UInt8) = isdigit8(b) ? b - UInt8('0') : (b >= UInt8('a') ? b - UInt8('a') + 0x0a : b - UInt8('A') + 0x0a)

function hex4(buf, i)
    return (UInt32(hexval(buf[i])) << 12) | (UInt32(hexval(buf[i + 1])) << 8) | (UInt32(hexval(buf[i + 2])) << 4) | UInt32(hexval(buf[i + 3]))
end

function appendcodepoint!(out::Vector{UInt8}, cp::UInt32)
    if cp < 0x80
        push!(out, UInt8(cp))
    elseif cp < 0x800
        push!(out, UInt8(0xC0 | (cp >> 6)), UInt8(0x80 | (cp & 0x3F)))
    elseif cp < 0x10000
        push!(out, UInt8(0xE0 | (cp >> 12)), UInt8(0x80 | ((cp >> 6) & 0x3F)), UInt8(0x80 | (cp & 0x3F)))
    else
        push!(out, UInt8(0xF0 | (cp >> 18)), UInt8(0x80 | ((cp >> 12) & 0x3F)), UInt8(0x80 | ((cp >> 6) & 0x3F)), UInt8(0x80 | (cp & 0x3F)))
    end
    return out
end

"""
    decodestring(buf, i, j) -> String

Decode the pre-scanned JSON string whose opening quote is at `buf[i]`, returning its WTF-8 form: a high
surrogate immediately followed by a low surrogate becomes one scalar; any other surrogate code unit is
kept as its own 3-byte sequence (plan decision 47). `j` is the position of the closing quote. Raw
bytes (already strict UTF-8) are copied as they are.
"""
function decodestring(buf::AbstractVector{UInt8}, i::Int, j::Int)
    # fast path: no escapes
    hasescape = false
    for k in i + 1:j - 1
        if buf[k] == UInt8('\\')
            hasescape = true
            break
        end
    end
    hasescape || return String(buf[i + 1:j - 1])
    out = Vector{UInt8}()
    sizehint!(out, j - i)
    k = i + 1
    while k < j
        b = buf[k]
        if b != UInt8('\\')
            push!(out, b)
            k += 1
            continue
        end
        e = buf[k + 1]
        if e == UInt8('u')
            cp = hex4(buf, k + 2)
            k += 6
            if 0xD800 <= cp <= 0xDBFF && k + 5 < j && buf[k] == UInt8('\\') && buf[k + 1] == UInt8('u')
                lo = hex4(buf, k + 2)
                if 0xDC00 <= lo <= 0xDFFF
                    cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00)
                    k += 6
                end
            end
            appendcodepoint!(out, cp)
        else
            push!(out, e == UInt8('b') ? 0x08 : e == UInt8('f') ? 0x0C : e == UInt8('n') ? 0x0A :
                       e == UInt8('r') ? 0x0D : e == UInt8('t') ? 0x09 : e)
            k += 2
        end
    end
    return String(out)
end

"""
    escapejson(io, s)

Write `s` as a JSON string literal, re-escaping every WTF-8 surrogate sequence as `\\uXXXX` so metadata
round-trips code-unit-exactly.
"""
function escapejson(io::IO, s::AbstractString)
    cu = codeunits(s)
    n = length(cu)
    print(io, '"')
    i = 1
    while i <= n
        b = cu[i]
        if b == UInt8('"')
            print(io, "\\\""); i += 1
        elseif b == UInt8('\\')
            print(io, "\\\\"); i += 1
        elseif b < 0x20
            if b == 0x08; print(io, "\\b") elseif b == 0x0C; print(io, "\\f") elseif b == 0x0A; print(io, "\\n")
            elseif b == 0x0D; print(io, "\\r") elseif b == 0x09; print(io, "\\t")
            else print(io, "\\u", string(b; base=16, pad=4)) end
            i += 1
        elseif b == 0xED && i + 2 <= n && cu[i + 1] >= 0xA0 && (cu[i + 1] & 0xC0) == 0x80 && (cu[i + 2] & 0xC0) == 0x80
            cp = (UInt32(b & 0x0F) << 12) | (UInt32(cu[i + 1] & 0x3F) << 6) | UInt32(cu[i + 2] & 0x3F)
            print(io, "\\u", string(cp; base=16, pad=4))
            i += 3
        else
            Base.write(io, b); i += 1
        end
    end
    print(io, '"')
    return nothing
end

# ---- recursive descent into frozen trees --------------------------------------------------------

mutable struct JSONReader{B<:AbstractVector{UInt8}}
    const buf::B
    const n::Int
    const maxdepth::Int
    const errfn::Any
    const budget::Union{Nothing,Budget}
    pos::Int
    depth::Int
    const lenient::Bool      # the non-JSON tokens NaN / Infinity / -Infinity (Java's decoder accepts them)
end

function skipws!(r::JSONReader)
    while r.pos <= r.n && isws(r.buf[r.pos])
        r.pos += 1
    end
    return nothing
end

function charge!(r::JSONReader, n::Int)
    r.budget === nothing || reserve!(r.budget, n)
    return nothing
end

"""
    parsejson(buf; maxbytes, maxdepth, errfn, budget=nothing) -> FrozenJSON

Pre-scan and parse one JSON document into a frozen tree (objects keep source key order in `order`,
duplicate keys are errors detected over decoded keys, numbers are raw `JSONNumber` tokens).
"""
function parsejson(buf::AbstractVector{UInt8}; maxbytes::Int, maxdepth::Int, errfn, budget::Union{Nothing,Budget}=nothing,
                   limitfn=(limit, observed, value) -> errfn("$limit exceeded ($observed > $value)", 0),
                   bytelimit::Symbol=:max_bytes, depthlimit::Symbol=:max_depth, lenient::Bool=false)
    prescan(buf, maxbytes, maxdepth, errfn; limitfn=limitfn, bytelimit=bytelimit, depthlimit=depthlimit, lenient=lenient)
    r = JSONReader(buf, length(buf), maxdepth, errfn, budget, 1, 0, lenient)
    skipws!(r)
    v = parsevalue!(r)
    skipws!(r)
    r.pos <= r.n && errfn("trailing content after the JSON document", r.pos)
    return v
end

parsejson(s::AbstractString; kw...) = parsejson(Vector{UInt8}(codeunits(s)); kw...)

function parsevalue!(r::JSONReader)
    r.pos <= r.n || r.errfn("unexpected end of input", r.pos)
    b = r.buf[r.pos]
    if b == UInt8('{')
        return parseobject!(r)
    elseif b == UInt8('[')
        return parsearray!(r)
    elseif b == UInt8('"')
        return parsestring!(r)
    elseif b == UInt8('t')
        r.pos += 4; return true
    elseif b == UInt8('f')
        r.pos += 5; return false
    elseif b == UInt8('n')
        r.pos += 4; return nothing
    elseif r.lenient && (b == UInt8('N') || b == UInt8('I') || (b == UInt8('-') && r.pos < r.n && r.buf[r.pos + 1] == UInt8('I')))
        lit = b == UInt8('N') ? "NaN" : (b == UInt8('I') ? "Infinity" : "-Infinity")
        r.pos += sizeof(lit)
        return b == UInt8('N') ? NaN : (b == UInt8('I') ? Inf : -Inf)
    elseif b == UInt8('-') || isdigit8(b)
        start = r.pos
        r.pos = scannumber(r.buf, r.pos, r.n, r.errfn)
        tok = String(r.buf[start:r.pos - 1])
        charge!(r, sizeof(tok) + 8)
        iv = parseinteger(tok)
        iv === nothing || return iv
        return JSONNumber(tok)
    else
        r.errfn("unexpected byte 0x$(string(b; base=16, pad=2))", r.pos)
    end
end

function parsestring!(r::JSONReader)
    start = r.pos
    j = scanstring(r.buf, r.pos, r.n, r.errfn) - 1   # closing quote
    r.pos = j + 1
    charge!(r, j - start + 16)
    return decodestring(r.buf, start, j)
end

function enter!(r::JSONReader)
    r.depth += 1
    r.depth <= r.maxdepth || r.errfn("JSON nesting depth exceeds $(r.maxdepth)", r.pos)
    return nothing
end

function parsearray!(r::JSONReader)
    enter!(r)
    r.pos += 1
    items = FrozenVector{Any}()
    charge!(r, 64)
    skipws!(r)
    if r.pos <= r.n && r.buf[r.pos] == UInt8(']')
        r.pos += 1
        r.depth -= 1
        return JSONArray(freeze!(items))
    end
    while true
        skipws!(r)
        charge!(r, 8)
        push!(items, parsevalue!(r))
        skipws!(r)
        r.pos <= r.n || r.errfn("unterminated array", r.pos)
        b = r.buf[r.pos]
        if b == UInt8(',')
            r.pos += 1
        elseif b == UInt8(']')
            r.pos += 1
            break
        else
            r.errfn("expected ',' or ']' in array", r.pos)
        end
    end
    r.depth -= 1
    return JSONArray(freeze!(items))
end

function parseobject!(r::JSONReader)
    enter!(r)
    r.pos += 1
    keys = String[]
    vals = Any[]
    spans = UnitRange{Int}[]
    charge!(r, 128)
    skipws!(r)
    if r.pos <= r.n && r.buf[r.pos] == UInt8('}')
        r.pos += 1
        r.depth -= 1
        return JSONObject(freeze!(FrozenDict{String,Any}()), freeze!(FrozenVector{String}()), freeze!(FrozenVector{UnitRange{Int}}()))
    end
    while true
        skipws!(r)
        (r.pos <= r.n && r.buf[r.pos] == UInt8('"')) || r.errfn("expected a string key", r.pos)
        k = parsestring!(r)
        skipws!(r)
        (r.pos <= r.n && r.buf[r.pos] == UInt8(':')) || r.errfn("expected ':' after object key", r.pos)
        r.pos += 1
        skipws!(r)
        charge!(r, 32)
        push!(keys, k)
        vstart = r.pos
        push!(vals, parsevalue!(r))
        push!(spans, vstart:r.pos - 1)
        skipws!(r)
        r.pos <= r.n || r.errfn("unterminated object", r.pos)
        b = r.buf[r.pos]
        if b == UInt8(',')
            r.pos += 1
        elseif b == UInt8('}')
            r.pos += 1
            break
        else
            r.errfn("expected ',' or '}' in object", r.pos)
        end
    end
    r.depth -= 1
    return buildobject(r, keys, vals, spans)
end

"""
    buildobject(r, keys, vals) -> JSONObject

Detect duplicate keys by sorting the decoded keys (no hashing; every compared byte charged to the
comparison rule) and build the sorted member dictionary plus the source order.
"""
function buildobject(r::JSONReader, keys::Vector{String}, vals::Vector{Any}, spans::Vector{UnitRange{Int}})
    n = length(keys)
    perm = collect(1:n)
    charge!(r, 8 * n)
    cmp = Ref(0)
    sort!(perm; alg=MergeSort, lt=(a, b) -> begin
        ka = keys[a]; kb = keys[b]
        cmp[] += min(sizeof(ka), sizeof(kb)) + 1
        isless(ka, kb)
    end)
    r.budget === nothing || addcompare!(r.budget, cmp[])
    for i in 2:n
        keys[perm[i]] == keys[perm[i - 1]] && r.errfn("duplicate object key \"$(keys[perm[i]])\"", r.pos)
    end
    sortedkeys = Vector{String}(undef, n)
    sortedvals = Vector{Any}(undef, n)
    for (i, p) in enumerate(perm)
        sortedkeys[i] = keys[p]
        sortedvals[i] = vals[p]
    end
    d = FrozenDict{String,Any}(sortedkeys, sortedvals, false)
    return JSONObject(freeze!(d), freeze!(FrozenVector{String}(keys, false)), freeze!(FrozenVector{UnitRange{Int}}(spans, false)))
end
