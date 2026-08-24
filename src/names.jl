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

function Base.:(==)(a::FullName, b::FullName)
    return a.name == b.name && a.namespace == b.namespace
end

function Base.hash(a::FullName, h::UInt)
    return hash(a.namespace, hash(a.name, hash(:FullName, h)))
end

function Base.isless(a::FullName, b::FullName)
    return isless(fullname(a), fullname(b))
end

function Base.show(io::IO, f::FullName)
    return print(io, "FullName(\"", fullname(f), "\")")
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
    for part in split(s, '.')
        isvalidname(part) || return false
    end
    return true
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
