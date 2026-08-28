# Representation storage formulas (plan §4.4 (b)): every charge for package-produced Julia storage is
# `header + per_element × n` with constants measured at package initialisation from canonical probe
# objects with `Base.summarysize`, so a Julia layout change is picked up rather than trusted. The same
# formulas are the oracle `heldbytes(x) ≥ Base.summarysize(x; exclude=Avro.Schema)` asserted by the
# tests for every member of `E`.

"""
    StorageConstants

Layout constants measured by `measurestorage` (the values recorded for Julia 1.10.11 and 1.12.6 are
`RECORDED_STORAGE`, asserted on every supported version).
"""
struct StorageConstants
    vector::Int       # `Vector` header
    string::Int       # empty `String`
    slot::Int         # one reference slot of a `Vector{Any}`
    tag::Int          # the type-tag byte per element of an isbits-`Union` vector
    bigint::Int       # `BigInt` beyond its limbs
    map::Int          # `Avro.Map` beyond its three vectors
    record::Int       # `Avro.Record` beyond its values vector (schema excluded)
    fixed::Int        # `Avro.Fixed` beyond its bytes vector (schema excluded)
    enumvalue::Int    # `Avro.EnumValue` (schema excluded)
    unionvalue::Int   # `Avro.UnionValue` beyond its boxed value
end

const RECORDED_STORAGE = StorageConstants(40, 8, 8, 1, 16, 24, 16, 16, 16, 16)
const STORAGE = Ref(RECORDED_STORAGE)
const OBJECT_HEADER = 8   # the type-tag word of every heap object (not counted by `summarysize`)

"Measure the storage constants from canonical probe objects (called from `__init__`)."
function measurestorage()
    ss = Base.summarysize
    vector = ss(UInt8[])
    slot = ss(Any[Int64(1)]) - ss([]) - sizeof(Int64)
    tag = (ss(Vector{Union{Missing,Int64}}(undef, 8)) - vector) ÷ 8 - sizeof(Int64)
    emptyrecord = Record(RecordSchema(FullName("Probe", ""), freeze!(FrozenVector{String}()), freeze!(FrozenVector{String}()), nothing, false,
                                      Props(), freeze!(FrozenVector{Field}()), freeze!(FrozenDict{String,Int}()), NodeMeta()), [], Val(:unchecked))
    fixedschema = FixedSchema(FullName("Probe", ""), freeze!(FrozenVector{String}()), freeze!(FrozenVector{String}()), 0, nothing, Props(), NodeMeta())
    enumschema = EnumSchema(FullName("Probe", ""), freeze!(FrozenVector{String}()), freeze!(FrozenVector{String}()), nothing,
                            freeze!(FrozenVector{String}(["a"], false)), nodefault, freeze!(FrozenDict{String,Int}()), Props(), NodeMeta())
    return StorageConstants(vector, ss(""), slot, tag, ss(big(1)) - sizeof(UInt64), ss(Map{Any}()) - 3 * vector,
                            ss(emptyrecord; exclude=Schema) - vector, ss(Fixed(fixedschema, UInt8[], Val(:unchecked)); exclude=Schema) - vector,
                            ss(EnumValue(enumschema, Int32(1), Val(:unchecked)); exclude=Schema), ss(UnionValue(1, nothing)))
end

# ---- formulas (the charges made by decoders, constructors and builders) ----------------------------

"Bytes of one element slot of a `Vector{T}`: the inline element size (isbits and immutable structs), an isbits-union payload plus its tag byte, or a reference."
function slotbytes(@nospecialize(T::Type))
    Base.isbitsunion(T) && return Base.elsize(Vector{T}) + STORAGE[].tag
    return Base.elsize(Vector{T})
end

"Bytes of one root value in the generic block-output representation."
function representationslot(@nospecialize(T::Type))
    Base.isbitsunion(T) && return slotbytes(T)
    isbitstype(T) && return sizeof(T)
    return STORAGE[].slot
end

function vectorbytes(@nospecialize(T::Type), n::Int)
    return STORAGE[].vector + n * slotbytes(T)
end

function bytesbytes(n::Int)
    return STORAGE[].vector + n
end

function stringbytes(n::Int)
    return STORAGE[].string + STORAGE[].slot + n
end

function boxbytes(@nospecialize(T::Type))
    return STORAGE[].slot + OBJECT_HEADER + sizeof(T)
end

function recordbytes(nfields::Int)
    return STORAGE[].record + vectorbytes(Any, nfields)
end

function fixedbytes(n::Int)
    return STORAGE[].fixed + bytesbytes(n)
end

function enumvaluebytes()
    return STORAGE[].enumvalue
end

function unionvaluebytes()
    return STORAGE[].unionvalue
end

function widedecimalbytes(nbytes::Int)
    return 16 + STORAGE[].bigint + 8 * (cld(nbytes, 8) + 2)   # struct, BigInt, limbs and two limbs of GMP slack (the negative path over-allocates)
end

"The `Avro.Map` parts `buildmap` allocates itself: the struct and the `npairs` permutation."
function mapshellbytes(npairs::Int)
    return STORAGE[].map + vectorbytes(Int32, npairs)
end

function mapbytes(@nospecialize(V::Type), npairs::Int)
    return mapshellbytes(npairs) + vectorbytes(String, npairs) + vectorbytes(V, npairs)
end

"The boxed-value charge of a record field or `Any` slot of static type `T` (0 for reference types)."
function boxcharge(@nospecialize(T::Type))
    isbitstype(T) && return boxbytes(T)
    T isa Union || return 0
    u = Base.nonmissingtype(T)
    return isbitstype(u) ? boxbytes(u) : 0
end

# ---- the oracle: the storage of a decoded value by the same formulas ---------------------------------

"""
    storagebytes(x) -> Int

The package's formula for the Julia storage of the generic value `x` (its own object and everything it
references; schema references excluded). `heldbytes(x)` is the charge of `x` held in an `Any` slot
(isbits values boxed).
"""
function storagebytes(x::String)
    return stringbytes(sizeof(x))
end

function storagebytes(x::Vector{UInt8})
    return bytesbytes(length(x))
end

function storagebytes(x::Fixed)
    return fixedbytes(length(x.bytes))
end

function storagebytes(::EnumValue)
    return enumvaluebytes()
end

function storagebytes(x::UnionValue)
    return unionvaluebytes() + heldbytes(x.value)
end

function storagebytes(x::WideDecimal)
    return 16 + STORAGE[].bigint + 8 * max(Int(x.unscaled.alloc), 1)
end

function storagebytes(x::Record)
    return recordbytes(capacity(getfield(x, :values))) + sum(heldbytes, getfield(x, :values); init=0)
end

function storagebytes(x::Vector{T}) where {T}
    return vectorbytes(T, capacity(x)) + sum(v -> elementbytes(T, v), x; init=0)
end

function storagebytes(x::Map{V}) where {V}
    shell = STORAGE[].map + vectorbytes(Int32, capacity(x.perm)) + vectorbytes(String, capacity(x.keys)) + vectorbytes(V, capacity(x.vals))
    return shell + sum(storagebytes, x.keys; init=0) + sum(v -> elementbytes(V, v), x.vals; init=0)
end

function storagebytes(x)
    return isbits(x) ? 0 : throw(ArgumentError("no storage formula for $(typeof(x))"))
end

function heldbytes(::Missing)
    return 0
end

function heldbytes(x)
    return isbits(x) ? boxbytes(typeof(x)) : storagebytes(x)
end

# The storage an element `x` adds beyond its slot in a `Vector{T}` / `Map{T}`. Identity-bearing structs
# stored inline are charged at production as well as for their slot (the decoder's charge model; also
# what Julia 1.10's `summarysize` reports for them).
function elementbytes(@nospecialize(T::Type), x)
    (isbitstype(T) || Base.isbitsunion(T)) && return 0
    (T === Any || T isa Union) && return heldbytes(x)
    return storagebytes(x)
end

# The retained slots of a vector (compacted maps keep their `npairs` capacity, which stays charged).
@static if VERSION >= v"1.11"
    function capacity(v::Vector)
        return length(v.ref.mem)
    end
else
    function capacity(v::Vector)
        return length(v)
    end
end
