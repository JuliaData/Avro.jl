# `FullName` and the Avro name grammar.

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

function Base.show(io::IO, f::FullName)
    print(io, "FullName(\"")
    writefullname(io, f)
    return print(io, "\")")
end

const PRIMITIVE_NAMES = ("null", "boolean", "int", "long", "float", "double", "bytes", "string")

function isnamestart(b::UInt8)
    return (b == UInt8('_')) || (UInt8('A') <= b <= UInt8('Z')) || (UInt8('a') <= b <= UInt8('z'))
end

function isnamechar(b::UInt8)
    return isnamestart(b) || (UInt8('0') <= b <= UInt8('9'))
end

"""
    isvalidname(s) -> Bool

`true` when `s` matches the Avro name grammar `[A-Za-z_][A-Za-z0-9_]*`.
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
    isreservedfullname(f::FullName) -> Bool

Primitive type names are reserved in the null namespace only (`a.int` is a valid fullname).
"""
function isreservedfullname(f::FullName)
    return isempty(f.namespace) && f.name in PRIMITIVE_NAMES
end
