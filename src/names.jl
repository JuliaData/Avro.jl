# Names (plan §4.2): the fullname algorithm, name grammar validation and namespace resolution.

"""
    FullName(name, namespace)

A named type's name and namespace; `fullname(x)` is `name` when the namespace is empty and
`namespace * "." * name` otherwise.
"""
struct FullName
    name::String
    namespace::String
end

function fullname(f::FullName)
    return isempty(f.namespace) ? f.name : string(f.namespace, ".", f.name)
end

function fullnamesize(f::FullName)
    isempty(f.namespace) && return sizeof(f.name)
    return checked_add(checked_add(sizeof(f.namespace), 1), sizeof(f.name))
end

function fullnamebyte(f::FullName, i::Int)
    namespacebytes = sizeof(f.namespace)
    if namespacebytes == 0
        return codeunit(f.name, i)
    elseif i <= namespacebytes
        return codeunit(f.namespace, i)
    elseif i == namespacebytes + 1
        return UInt8('.')
    end
    return codeunit(f.name, i - namespacebytes - 1)
end

function fullnamecompare(a::FullName, b::FullName)
    na = fullnamesize(a)
    nb = fullnamesize(b)
    for i in 1:min(na, nb)
        ai = fullnamebyte(a, i)
        bi = fullnamebyte(b, i)
        ai == bi || return ai < bi ? -1 : 1
    end
    return cmp(na, nb)
end

function fullnamecompare(a::FullName, b::AbstractString)
    na = fullnamesize(a)
    nb = sizeof(b)
    bytes = codeunits(b)
    for i in 1:min(na, nb)
        ai = fullnamebyte(a, i)
        bi = bytes[i]
        ai == bi || return ai < bi ? -1 : 1
    end
    return cmp(na, nb)
end

function fullnamecompare(a::AbstractString, b::FullName)
    return -fullnamecompare(b, a)
end

function fullnamecomparisonwork(a::FullName, b::FullName)
    n = min(fullnamesize(a), fullnamesize(b))
    for i in 1:n
        fullnamebyte(a, i) == fullnamebyte(b, i) || return i
    end
    return n + 1
end

function fullnamecomparisonwork(a::FullName, b::AbstractString)
    n = min(fullnamesize(a), sizeof(b))
    bytes = codeunits(b)
    for i in 1:n
        fullnamebyte(a, i) == bytes[i] || return i
    end
    return n + 1
end

function fullnameequal(a::FullName, b::FullName)
    return a.name == b.name && a.namespace == b.namespace
end

function fullnameequal(a::FullName, b::AbstractString)
    return fullnamecompare(a, b) == 0
end

function budgetedfullnameequal(a::FullName, b::FullName, budget::Union{Nothing,Budget})
    budget === nothing || addcompare!(budget, fullnamecomparisonwork(a, b))
    return fullnameequal(a, b)
end

function budgetedfullnameequal(a::FullName, b::AbstractString, budget::Union{Nothing,Budget})
    budget === nothing || addcompare!(budget, fullnamecomparisonwork(a, b))
    return fullnameequal(a, b)
end

function writefullname(io::IO, f::FullName)
    isempty(f.namespace) || print(io, f.namespace, '.')
    return print(io, f.name)
end

function Base.:(==)(a::FullName, b::FullName)
    return a.name == b.name && a.namespace == b.namespace
end

function Base.hash(a::FullName, h::UInt)
    return hash(a.namespace, hash(a.name, hash(:FullName, h)))
end

function Base.isless(a::FullName, b::FullName)
    return fullnamecompare(a, b) < 0
end

function Base.isless(a::FullName, b::AbstractString)
    return fullnamecompare(a, b) < 0
end

function Base.isless(a::AbstractString, b::FullName)
    return fullnamecompare(a, b) < 0
end

function Base.show(io::IO, f::FullName)
    print(io, "FullName(\"")
    writefullname(io, f)
    return print(io, "\")")
end

function keycomparisonwork(a::FullName, b::FullName)
    return fullnamecomparisonwork(a, b)
end

function keycomparisonwork(a::FullName, b::AbstractString)
    return fullnamecomparisonwork(a, b)
end

function keycomparisonwork(a::AbstractString, b::FullName)
    return fullnamecomparisonwork(b, a)
end

function keymatches(a::FullName, b::FullName)
    return fullnameequal(a, b)
end

function keymatches(a::FullName, b::AbstractString)
    return fullnameequal(a, b)
end

function keymatches(a::AbstractString, b::FullName)
    return fullnameequal(b, a)
end

const PRIMITIVE_NAMES = ("null", "boolean", "int", "long", "float", "double", "bytes", "string")
const COMPLEX_KEYWORDS = ("record", "error", "enum", "array", "map", "fixed")

function isnamestart(b::UInt8)
    return (b == UInt8('_')) || (UInt8('A') <= b <= UInt8('Z')) || (UInt8('a') <= b <= UInt8('z'))
end

function isnamechar(b::UInt8)
    return isnamestart(b) || (UInt8('0') <= b <= UInt8('9'))
end

"""
    isvalidname(s) -> Bool

`true` when `s` matches the Avro name grammar `[A-Za-z_][A-Za-z0-9_]*` (plan §3).
"""
function isvalidname(s::AbstractString)
    cu = codeunits(s)
    isempty(cu) && return false
    isnamestart(cu[1]) || return false
    for i in 2:length(cu)
        isnamechar(cu[i]) || return false
    end
    return true
end

"""
    isvalidnamespace(s) -> Bool

`true` for the empty namespace or a dot-separated sequence of valid names.
"""
function isvalidnamespace(s::AbstractString)
    isempty(s) && return true
    atstart = true
    for byte in codeunits(s)
        if atstart
            isnamestart(byte) || return false
            atstart = false
        elseif byte == UInt8('.')
            atstart = true
        else
            isnamechar(byte) || return false
        end
    end
    return !atstart
end

"""
    splitfullname(s) -> (name, namespace)

Split a (possibly dotted) name: the text after the last dot is the name, the text before it the
namespace (`""` when there is no dot).
"""
function splitfullname(s::AbstractString)
    i = findlast('.', s)
    i === nothing && return (String(s), "")
    return (String(s[nextind(s, i):end]), String(s[1:prevind(s, i)]))
end

"""
    resolvefullname(name, namespace, enclosing) -> FullName

The spec's fullname algorithm: a dotted `name` wins and `namespace` is ignored; a simple name takes the
explicit `namespace` when given, else the enclosing namespace.
"""
function resolvefullname(name::AbstractString, namespace::Union{Nothing,AbstractString}, enclosing::AbstractString)
    n, ns = splitfullname(name)
    isempty(ns) || return FullName(n, ns)
    namespace === nothing && return FullName(n, String(enclosing))
    return FullName(n, String(namespace))
end

"""
    resolvereference(ref, enclosing) -> String

A named-type reference: a dotted reference is used as written; a simple one is qualified with the
enclosing namespace.
"""
function resolvereference(ref::AbstractString, enclosing::AbstractString)
    n, ns = splitfullname(ref)
    isempty(ns) || return String(ref)
    isempty(enclosing) && return n
    return string(enclosing, ".", n)
end

"""
    normalizealias(alias, namespace) -> String

A type alias is normalised to a fullname: a relative alias takes the namespace of the type it aliases
(plan §4.2).
"""
function normalizealias(alias::AbstractString, namespace::AbstractString)
    n, ns = splitfullname(alias)
    isempty(ns) || return String(alias)
    isempty(namespace) && return n
    return string(namespace, ".", n)
end

"""
    isreservedfullname(f::FullName) -> Bool

Primitive type names are reserved in the null namespace only (`a.int` is a valid fullname).
"""
function isreservedfullname(f::FullName)
    return isempty(f.namespace) && f.name in PRIMITIVE_NAMES
end
