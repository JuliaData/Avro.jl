# Symbol admission: Julia never frees interned `Symbol`s. By default names and `Symbol` values are
# interned directly (a file's names are bounded by its schema limits). A caller-owned `SymbolAdmission`
# caps the strings interned through it, for services that read untrusted files for a long time.

"Index bytes charged per admitted name beyond its string (its key and value slots)."
const ADMISSION_SLOT_BYTES = 16

"""
    Avro.SymbolAdmission(; max_names=1_000_000, max_bytes=64 << 20)

A caller-owned cap on the `Symbol`s interned through `names=` (Tables column names, typed `Symbol`
values). A string is interned when it was admitted before or the table has room; strings already
admitted do not count twice. `max_bytes` bounds the admitted strings plus 16 bytes of index per name.
Exceeding `max_names` or `max_bytes` raises `LimitError` and leaves the table unchanged.

The table is shared state by design: whether a read succeeds depends on what the table admitted
before. Without `names=`, nothing is consulted and names are interned directly.
"""
mutable struct SymbolAdmission
    const lock::ReentrantLock
    const max_names::Int
    const max_bytes::Int
    const index::NameIndex{Nothing}     # sorted runs: no hashing of untrusted strings
    bytes::Int
end

function SymbolAdmission(; max_names::Integer=1_000_000, max_bytes::Integer=64 << 20)
    max_names >= 0 || throw(ArgumentError("max_names must be ≥ 0"))
    max_bytes >= 0 || throw(ArgumentError("max_bytes must be ≥ 0"))
    return SymbolAdmission(ReentrantLock(), Int(max_names), Int(max_bytes), NameIndex{Nothing}(nothing), 0)
end

function Base.length(a::SymbolAdmission)
    return lock(() -> length(a.index), a.lock)
end

"Copy caller text into one retained String allocation (the caller's string may be a view)."
function admissionstringcopy(s::AbstractString)
    n = sizeof(s)
    n == 0 && return ""
    bytes = codeunits(s)
    return GC.@preserve s bytes unsafe_string(pointer(bytes), n)
end

"""
    admit!(names, s::AbstractString) -> Symbol

`Symbol(s)`, after admitting `s` to `names` when it is a `SymbolAdmission`.
"""
function admit!(a::SymbolAdmission, s::AbstractString)
    lock(a.lock) do
        haskey(a.index, s) && return nothing
        count = length(a.index) + 1
        count <= a.max_names || throw(LimitError(:max_names, count, a.max_names, :max_names, :decode))
        bytes = checked_add(a.bytes, stringbytes(sizeof(s)) + ADMISSION_SLOT_BYTES)
        bytes <= a.max_bytes || throw(LimitError(:max_bytes, bytes, a.max_bytes, :max_bytes, :decode))
        insert!(a.index, admissionstringcopy(s), nothing, nothing)
        a.bytes = bytes
        return nothing
    end
    return Symbol(s)
end

function admit!(::Nothing, s::AbstractString)
    return Symbol(s)
end

"""
    admission(names) -> Union{Nothing,SymbolAdmission}

Normalise the `names=` keyword: `nothing` (intern directly) or a `SymbolAdmission`.
"""
function admission(a::SymbolAdmission)
    return a
end

function admission(::Nothing)
    return nothing
end

function admission(x)
    throw(ArgumentError("`names` must be `nothing` or an Avro.SymbolAdmission"))
end
