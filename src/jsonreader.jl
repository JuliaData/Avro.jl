# Avro's own JSON reader: size, BOM and strict UTF-8 are checked first, then one recursive descent builds
# plain values with raw number tokens and WTF-8 strings. JSON.jl is never used for parsing.

function isws(b::UInt8)
    return b == UInt8(' ') || b == UInt8('\t') || b == UInt8('\n') || b == UInt8('\r')
end

function isdigit8(b::UInt8)
    return UInt8('0') <= b <= UInt8('9')
end

function ishex8(b::UInt8)
    return isdigit8(b) || (UInt8('a') <= b <= UInt8('f')) || (UInt8('A') <= b <= UInt8('F'))
end

"Strict UTF-8 validity (Base rejects overlongs, surrogates and anything above U+10FFFF)."
function isstrictutf8(s::AbstractString)
    return isvalid(String, codeunits(s))
end

function isstrictutf8(bytes::AbstractVector{UInt8})
    return isvalid(String, bytes)
end

function isstrictutf8(s::String)
    return isvalid(s)
end

# A `DataString` checks its bytes where they are: a long one's in its buffer, a short one's in its payload.
function isstrictutf8(s::DataString)
    n = ncodeunits(s)
    if n > DataStrings.INLINE_MAX
        pos = DataStrings.payloadpos(s.p)
        return isvalid(String, view(s.data, pos:pos + n - 1))
    end
    ((s.p.a >> 32) | s.p.b) & 0x8080808080808080 == 0 && return true      # ASCII, zero padding included
    return isvalid(String, codeunits(s))
end

const HEX_DIGITS = codeunits("0123456789abcdef")

"Write one JSON `\\uXXXX` escape without allocating a temporary hexadecimal string."
function writehexescape(io::IO, value::Integer)
    v = UInt32(value)
    Base.write(io, UInt8('\\'), UInt8('u'),
        HEX_DIGITS[Int((v >> 12) & 0x0f) + 1],
        HEX_DIGITS[Int((v >> 8) & 0x0f) + 1],
        HEX_DIGITS[Int((v >> 4) & 0x0f) + 1],
        HEX_DIGITS[Int(v & 0x0f) + 1])
    return nothing
end

"""
    JSONNumber(text)

A JSON number token that is not an `Int64` (a fraction, an exponent or an overflowing integer), kept as
its source text so schemas re-serialise it verbatim. Equality and hashing are lexical.
"""
struct JSONNumber
    text::String
end

function Base.:(==)(a::JSONNumber, b::JSONNumber)
    return a.text == b.text
end

function Base.hash(a::JSONNumber, h::UInt)
    return hash(a.text, hash(:JSONNumber, h))
end

# ---- lexical scanners ---

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
    parsefloat(T, token) -> T

Correctly rounded, linear-time parse of a JSON number token as `Float32` or `Float64` (overflow gives
±Inf, underflow ±0.0 or a subnormal, `-0.0` preserved, as Java's `parseDouble`).
"""
function parsefloat(::Type{T}, tok::AbstractString) where {T<:Union{Float32,Float64}}
    v = T === Float32 ? tryparsefloat32(tok) : tryparse(Float64, tok)
    v === nothing || return v
    # Julia's parser rejects out-of-range tokens; BigFloat keeps their sign and magnitude.
    return T(BigFloat(tok; precision=256))
end

function tryparsefloat32(tok::AbstractString)
    @static if Sys.iswindows()
        # Julia parses Float32 through Float64 on Windows, which can round twice. UCRT's `strtof` rounds
        # once, and its ±Inf and ±0 for out-of-range tokens equal the BigFloat fallback's results.
        text = tok isa String ? tok : String(tok)
        endpoint = Ref{Ptr{UInt8}}(C_NULL)
        value = GC.@preserve text begin
            start = pointer(text)
            parsed = ccall((:strtof, "ucrtbase"), Cfloat,
                           (Ptr{UInt8}, Ptr{Ptr{UInt8}}), start, endpoint)
            endpoint[] == start + sizeof(text) ? parsed : nothing
        end
        return value
    else
        return tryparse(Float32, tok)
    end
end

# ---- WTF-8 string decoding ---------------------------------------------------------------------

function hexval(b::UInt8)
    return isdigit8(b) ? b - UInt8('0') : (b >= UInt8('a') ? b - UInt8('a') + 0x0a : b - UInt8('A') + 0x0a)
end

function hex4(buf, i)
    return (UInt32(hexval(buf[i])) << 12) | (UInt32(hexval(buf[i + 1])) << 8) | (UInt32(hexval(buf[i + 2])) << 4) | UInt32(hexval(buf[i + 3]))
end

function writecodepoint!(out::Vector{UInt8}, position::Int, cp::UInt32)
    if cp < 0x80
        out[position] = UInt8(cp)
        return position + 1
    elseif cp < 0x800
        out[position] = UInt8(0xC0 | (cp >> 6))
        out[position + 1] = UInt8(0x80 | (cp & 0x3F))
        return position + 2
    elseif cp < 0x10000
        out[position] = UInt8(0xE0 | (cp >> 12))
        out[position + 1] = UInt8(0x80 | ((cp >> 6) & 0x3F))
        out[position + 2] = UInt8(0x80 | (cp & 0x3F))
        return position + 3
    else
        out[position] = UInt8(0xF0 | (cp >> 18))
        out[position + 1] = UInt8(0x80 | ((cp >> 12) & 0x3F))
        out[position + 2] = UInt8(0x80 | ((cp >> 6) & 0x3F))
        out[position + 3] = UInt8(0x80 | (cp & 0x3F))
        return position + 4
    end
end

"""
    decodestring(buf, i, j) -> String

Decode the pre-scanned JSON string whose opening quote is at `buf[i]` and closing quote at `buf[j]`,
returning its WTF-8 form: a high surrogate immediately followed by a low surrogate becomes one scalar;
any other surrogate code unit is kept as its own 3-byte sequence. Raw bytes (already strict UTF-8) are
copied as they are.
"""
function decodestring(buf::AbstractVector{UInt8}, i::Int, j::Int)
    hasescape = false
    for k in i + 1:j - 1
        if buf[k] == UInt8('\\')
            hasescape = true
            break
        end
    end
    if !hasescape
        n = j - i - 1
        return n == 0 ? "" : GC.@preserve buf unsafe_string(pointer(buf, i + 1), n)
    end
    out = Vector{UInt8}(undef, j - i - 1)   # an escape never decodes longer than it is written
    position = 1
    k = i + 1
    while k < j
        b = buf[k]
        if b != UInt8('\\')
            out[position] = b
            position += 1
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
            position = writecodepoint!(out, position, cp)
        else
            out[position] = e == UInt8('b') ? 0x08 : e == UInt8('f') ? 0x0C :
                            e == UInt8('n') ? 0x0A : e == UInt8('r') ? 0x0D :
                            e == UInt8('t') ? 0x09 : e
            position += 1
            k += 2
        end
    end
    return String(resize!(out, position - 1))
end

"""
    escapejson(io, s)

Write `s` as a JSON string literal, re-escaping every WTF-8 surrogate sequence as `\\uXXXX` so metadata
round-trips code-unit-exactly.
"""
function escapejson(io::IO, s::AbstractString)
    print(io, '"')
    cu = codeunits(s)
    n = length(cu)
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
            else writehexescape(io, b) end
            i += 1
        elseif b == 0xED && i + 2 <= n && cu[i + 1] >= 0xA0 && (cu[i + 1] & 0xC0) == 0x80 && (cu[i + 2] & 0xC0) == 0x80
            cp = (UInt32(b & 0x0F) << 12) | (UInt32(cu[i + 1] & 0x3F) << 6) | UInt32(cu[i + 2] & 0x3F)
            writehexescape(io, cp)
            i += 3
        else
            Base.write(io, b); i += 1
        end
    end
    print(io, '"')
    return nothing
end

# ---- recursive descent ---------------------------------------------------------------------------

mutable struct JSONReader{B<:AbstractVector{UInt8},F}
    const buf::B
    const n::Int
    const maxdepth::Int
    const errfn::F
    pos::Int
    depth::Int
    const lenient::Bool      # accept the non-JSON tokens NaN / Infinity / -Infinity
end

function skipws!(r::JSONReader)
    while r.pos <= r.n && isws(r.buf[r.pos])
        r.pos += 1
    end
    return nothing
end

"""
    parsejson(buf; limits=Limits(), errfn, lenient=false) -> value

Pre-scan and parse one JSON document into plain values: `nothing`, `Bool`, `Int64`, `JSONNumber` (any
other number token), `String` (WTF-8), `Vector{Any}` and `JSON.Object{String,Any}` (source key order;
duplicate keys are an error). `limits.max_bytes` and `limits.max_depth` bound the document (`LimitError`);
every other defect goes through `errfn(msg, pos)`, which must throw. `lenient` admits the tokens `NaN`,
`Infinity` and `-Infinity` as `Float64` values.
"""
function parsejson(buf::AbstractVector{UInt8}; limits::Limits=Limits(), errfn, lenient::Bool=false)
    n = length(buf)
    n <= limits.max_bytes || throw(LimitError(:max_bytes, n, limits.max_bytes))
    n >= 3 && buf[1] == 0xEF && buf[2] == 0xBB && buf[3] == 0xBF && errfn("a byte-order mark is not allowed", 1)
    isvalid(String, buf) || errfn("invalid UTF-8 sequence", 1)
    r = JSONReader(buf, n, limits.max_depth, errfn, 1, 0, lenient)
    skipws!(r)
    r.pos <= n || errfn("empty JSON document", 1)
    v = parsevalue!(r)
    skipws!(r)
    r.pos <= r.n && errfn("trailing content after the JSON document", r.pos)
    return v
end

function parsejson(s::AbstractString; kw...)
    return parsejson(Vector{UInt8}(codeunits(s)); kw...)
end

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
        r.pos = scanliteral(r.buf, r.pos, r.n, "true", r.errfn); return true
    elseif b == UInt8('f')
        r.pos = scanliteral(r.buf, r.pos, r.n, "false", r.errfn); return false
    elseif b == UInt8('n')
        r.pos = scanliteral(r.buf, r.pos, r.n, "null", r.errfn); return nothing
    elseif r.lenient && (b == UInt8('N') || b == UInt8('I') || (b == UInt8('-') && r.pos < r.n && r.buf[r.pos + 1] == UInt8('I')))
        lit = b == UInt8('N') ? "NaN" : (b == UInt8('I') ? "Infinity" : "-Infinity")
        r.pos = scanliteral(r.buf, r.pos, r.n, lit, r.errfn)
        return b == UInt8('N') ? NaN : (b == UInt8('I') ? Inf : -Inf)
    elseif b == UInt8('-') || isdigit8(b)
        start = r.pos
        r.pos = scannumber(r.buf, r.pos, r.n, r.errfn)
        buf = r.buf
        tok = GC.@preserve buf unsafe_string(pointer(buf, start), r.pos - start)
        iv = tryparse(Int64, tok)
        return iv === nothing ? JSONNumber(tok) : iv
    else
        r.errfn("unexpected byte 0x$(string(b; base=16, pad=2))", r.pos)
    end
end

function parsestring!(r::JSONReader)
    start = r.pos
    j = scanstring(r.buf, r.pos, r.n, r.errfn) - 1   # closing quote
    r.pos = j + 1
    return decodestring(r.buf, start, j)
end

function enter!(r::JSONReader)
    r.depth += 1
    r.depth <= r.maxdepth || throw(LimitError(:max_depth, r.depth, r.maxdepth))
    return nothing
end

function parsearray!(r::JSONReader)
    enter!(r)
    r.pos += 1
    items = Any[]
    skipws!(r)
    if r.pos <= r.n && r.buf[r.pos] == UInt8(']')
        r.pos += 1
        r.depth -= 1
        return items
    end
    while true
        skipws!(r)
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
    return items
end

function parseobject!(r::JSONReader)
    enter!(r)
    r.pos += 1
    skipws!(r)
    if r.pos <= r.n && r.buf[r.pos] == UInt8('}')
        r.pos += 1
        r.depth -= 1
        return JSON.Object{String,Any}()
    end
    members = Pair{String,Any}[]
    seen = Set{String}()
    while true
        skipws!(r)
        keystart = r.pos
        (r.pos <= r.n && r.buf[r.pos] == UInt8('"')) || r.errfn("expected a string key", r.pos)
        k = parsestring!(r)
        k in seen && r.errfn("duplicate object key $(quoted(k))", keystart)
        push!(seen, k)
        skipws!(r)
        (r.pos <= r.n && r.buf[r.pos] == UInt8(':')) || r.errfn("expected ':' after object key", r.pos)
        r.pos += 1
        skipws!(r)
        push!(members, Pair{String,Any}(k, parsevalue!(r)))
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
    return JSON.Object{String,Any}(members)
end
