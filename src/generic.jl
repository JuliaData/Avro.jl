# The generic value model: `Record`, `EnumValue` and `Fixed` carry their schema identity, `UnionValue`
# forces a union branch on encode, and `juliatype` maps every schema node to the closed set of Julia
# types a generic decode produces.

"""
    Avro.Record(schema::RecordSchema, values)

The generic record: field values in schema order. A `Record` is a `Tables.AbstractRow`, so fields read
as `record.name`, `record[:name]`, `record["name"]` or `record[i]`; `Avro.schema(record)` is the
record schema (a field named `schema` wins the property). Equality is structural.
"""
struct Record <: Tables.AbstractRow
    schema::RecordSchema
    values::Vector{Any}
    function Record(schema::RecordSchema, values::Vector{Any}, ::Val{:unchecked})
        return new(schema, values)
    end
end

function Record(schema::RecordSchema, values)
    nf = length(schema.fields)
    vs = Vector{Any}(undef, nf)
    i = 0
    for v in values
        i += 1
        i <= nf || throw(ArgumentError("record \"$(fullname(schema))\" has $nf fields, got more values"))
        vs[i] = v
    end
    i == nf || throw(ArgumentError("record \"$(fullname(schema))\" has $nf fields, got $i values"))
    return Record(schema, vs, Val(:unchecked))
end

function Tables.columnnames(r::Record)
    return getfield(r, :schema).fieldnames
end

function Tables.getcolumn(r::Record, i::Int)
    return getfield(r, :values)[i]
end

function Tables.getcolumn(r::Record, nm::Symbol)
    i = get(getfield(r, :schema).fieldindex, nm, 0)
    i == 0 && throw(KeyError(nm))
    return getfield(r, :values)[i]
end

function Base.getindex(r::Record, name::AbstractString)
    return Tables.getcolumn(r, Symbol(name))
end

function Base.haskey(r::Record, name::AbstractString)
    return haskey(getfield(r, :schema).fieldindex, Symbol(name))
end

function Base.get(r::Record, name::AbstractString, default)
    return haskey(r, name) ? r[name] : default
end

function Base.NamedTuple(r::Record)
    schema = getfield(r, :schema)
    return NamedTuple{Tuple(schema.fieldnames)}(Tuple(getfield(r, :values)))
end

function Base.:(==)(a::Record, b::Record)
    return getfield(a, :schema).name == getfield(b, :schema).name && getfield(a, :values) == getfield(b, :values)
end

function Base.isequal(a::Record, b::Record)
    return getfield(a, :schema).name == getfield(b, :schema).name && isequal(getfield(a, :values), getfield(b, :values))
end

function Base.hash(a::Record, h::UInt)
    return hash(getfield(a, :values), hash(getfield(a, :schema).name, hash(:AvroRecord, h)))
end

function Base.show(io::IO, r::Record)
    print(io, "Avro.Record(")
    writefullname(io, getfield(r, :schema).name)
    print(io, ": ")
    for (i, f) in enumerate(getfield(r, :schema).fields)
        i > 1 && print(io, ", ")
        print(io, f.name, "=")
        show(io, getfield(r, :values)[i])
    end
    print(io, ")")
    return nothing
end

function Base.show(io::IO, ::MIME"text/plain", r::Record)
    return show(io, r)
end

"""
    Avro.EnumValue(schema::EnumSchema, index::Integer)
    Avro.EnumValue(schema::EnumSchema, symbol::Union{AbstractString,Symbol})

A generic enum value: an `AbstractString` holding its symbol, so it compares, hashes, prints and converts
like that string, and it keeps its schema for encoding (`Avro.ordinal(x)` is the zero-based wire value,
`index` the 1-based position into `schema.symbols`).
"""
struct EnumValue <: AbstractString
    schema::EnumSchema
    index::Int32
    function EnumValue(schema::EnumSchema, index::Int32, ::Val{:unchecked})
        return new(schema, index)
    end
end

function EnumValue(schema::EnumSchema, index::Integer)
    1 <= index <= length(schema.symbols) ||
        throw(ArgumentError("enum \"$(fullname(schema))\" has $(length(schema.symbols)) symbols; index $index is out of range"))
    return EnumValue(schema, Int32(index), Val(:unchecked))
end

function EnumValue(schema::EnumSchema, symbol::Union{AbstractString,Symbol})
    i = get(schema.symbolindex, String(symbol), 0)
    i == 0 && throw(ArgumentError("$(repr(diagnostictext(symbol))) is not a symbol of enum \"$(fullname(schema))\""))
    return EnumValue(schema, Int32(i), Val(:unchecked))
end

function Base.String(x::EnumValue)
    return x.schema.symbols[x.index]
end

Base.ncodeunits(x::EnumValue) = ncodeunits(String(x))
Base.codeunit(::EnumValue) = UInt8
Base.codeunit(x::EnumValue, i::Integer) = codeunit(String(x), i)
Base.isvalid(x::EnumValue, i::Int) = isvalid(String(x), i)
Base.iterate(x::EnumValue, i::Int=1) = iterate(String(x), i)

function Base.show(io::IO, x::EnumValue)
    print(io, "Avro.EnumValue(")
    writefullname(io, x.schema.name)
    return print(io, ".", String(x), ")")
end

"""
    Avro.Fixed(schema::FixedSchema, bytes)

A generic fixed value: an `AbstractVector{UInt8}` of exactly `schema.size` copied bytes, so it compares
and hashes like any byte vector, and it keeps its schema for encoding.
"""
struct Fixed <: AbstractVector{UInt8}
    schema::FixedSchema
    bytes::Vector{UInt8}
    function Fixed(schema::FixedSchema, bytes::Vector{UInt8}, ::Val{:unchecked})
        return new(schema, bytes)
    end
end

function Fixed(schema::FixedSchema, bytes::AbstractVector{UInt8})
    length(bytes) == schema.size || throw(ArgumentError("fixed \"$(fullname(schema))\" has size $(schema.size), got $(length(bytes)) bytes"))
    return Fixed(schema, copyto!(Vector{UInt8}(undef, length(bytes)), bytes), Val(:unchecked))
end

Base.size(x::Fixed) = size(x.bytes)
Base.getindex(x::Fixed, i::Int) = x.bytes[i]
Base.IndexStyle(::Type{Fixed}) = IndexLinear()

function Base.show(io::IO, x::Fixed)
    print(io, "Avro.Fixed(")
    writefullname(io, x.schema.name)
    return print(io, ": 0x", bytes2hex(x.bytes), ")")
end

function Base.show(io::IO, ::MIME"text/plain", x::Fixed)
    return show(io, x)
end

"""
    Avro.UnionValue(index::Integer, value)

Forces the union branch `value` is encoded under: `index` is the 1-based branch position
(`Avro.ordinal(x)` is the wire index). Decoding never produces `UnionValue`s: union values decode to
the bare value of their branch, and encoding selects the branch from the value when no `UnionValue`
is given.
"""
struct UnionValue
    index::Int
    value::Any
    function UnionValue(index::Integer, value)
        index >= 1 || throw(ArgumentError("union branch index must be ≥ 1 (1-based), got $index"))
        return new(Int(index), value)
    end
end

function Base.:(==)(a::UnionValue, b::UnionValue)
    return a.index == b.index && a.value == b.value
end

function Base.isequal(a::UnionValue, b::UnionValue)
    return a.index == b.index && isequal(a.value, b.value)
end

function Base.hash(a::UnionValue, h::UInt)
    return hash(a.value, hash(a.index, hash(:AvroUnion, h)))
end

function Base.show(io::IO, x::UnionValue)
    return (print(io, "Avro.UnionValue(", x.index, ", "); show(io, x.value); print(io, ")"))
end

"""
    Avro.ordinal(x::EnumValue) / Avro.ordinal(x::UnionValue) -> Int

The zero-based wire ordinal of an enum symbol or union branch.
"""
function ordinal(x::EnumValue)
    return Int(x.index) - 1
end

function ordinal(x::UnionValue)
    return x.index - 1
end

# ---- the generic Julia type of a schema --------------------------------------------------

function juliatype(@nospecialize(s::Schema))
    s isa NullSchema && return Missing
    s isa BooleanSchema && return Bool
    s isa IntSchema && return s.logical isa DateLogical ? Date : s.logical isa TimeMillis ? Time : Int32
    s isa LongSchema && return longjuliatype(s.logical)
    s isa FloatSchema && return Float32
    s isa DoubleSchema && return Float64
    s isa BytesSchema && return s.logical isa DecimalLogical ? decimaltype(s.logical) : Vector{UInt8}
    s isa StringSchema && return s.logical isa UUIDLogical ? UUID : String
    if s isa FixedSchema
        l = s.logical
        l isa DecimalLogical && return decimaltype(l)
        l isa UUIDLogical && return UUID
        l isa DurationLogical && return Duration
        return Fixed
    end
    s isa EnumSchema && return EnumValue
    s isa RecordSchema && return Record
    s isa ArraySchema && return Vector{elementtype(s.items)}
    s isa MapSchema && return Dict{String,elementtype(s.values)}
    nullable = nullablebranch(s::UnionSchema)
    nullable == 0 && return Any
    return Union{Missing,juliatype(s.branches[3 - nullable])}
end

function longjuliatype(@nospecialize(l))
    l isa TimeMicros && return Time
    l isa TimestampMillis && return Timestamp{Millisecond}
    l isa TimestampMicros && return Timestamp{Microsecond}
    l isa TimestampNanos && return Timestamp{Nanosecond}
    l isa LocalTimestampMillis && return LocalTimestamp{Millisecond}
    l isa LocalTimestampMicros && return LocalTimestamp{Microsecond}
    l isa LocalTimestampNanos && return LocalTimestamp{Nanosecond}
    return Int64
end

function decimaltype(l::DecimalLogical)
    return l.precision <= 38 ? Decimal : WideDecimal
end

# ---- the closed value set ----------------------------------------------------------------

"""
    Avro.LEAF_TYPES

The leaf types of the generic value model.
"""
const LEAF_TYPES = (Missing, Bool, Int32, Int64, Float32, Float64, Vector{UInt8}, String, Fixed, EnumValue, Decimal, WideDecimal,
                    UUID, Date, Time, Timestamp{Millisecond}, Timestamp{Microsecond}, Timestamp{Nanosecond},
                    LocalTimestamp{Millisecond}, LocalTimestamp{Microsecond}, LocalTimestamp{Nanosecond}, Duration)

"""
    Avro.valuetypes() -> Vector{Type}

The closed set of types a generic decode can produce: the leaves, `Record`, `Any` (the bare values
of unions with more than one non-null branch), `Vector{x}`/`Dict{String,x}` for `x` a leaf, `Record` or
`Union{Missing,y}`, `Vector{Any}`, `Dict{String,Any}`, and `Union{Missing,x}` for every non-missing
member.
"""
function valuetypes()
    elements = Any[LEAF_TYPES..., Record]
    nullable = Any[Union{Missing,x} for x in (LEAF_TYPES[2:end]..., Record)]
    composites = Any[Record, Any]
    for x in (elements..., nullable...)
        push!(composites, Vector{x})
        push!(composites, Dict{String,x})
    end
    push!(composites, Vector{Any})
    push!(composites, Dict{String,Any})
    all = Any[LEAF_TYPES..., composites...]
    for x in (LEAF_TYPES[2:end]..., composites...)
        push!(all, Union{Missing,x})
    end
    return unique(all)
end

"""
    narrowelement(t::Type) -> Type

The element type the generic model stores for values of type `t` inside an array or map (one level
of typed nesting): a leaf, `Record` or `Union{Missing,y}` for a non-missing leaf or `Record` is kept;
every other type (nested arrays and maps, bare union values, abstract joins) is stored as `Any`.
"""
function narrowelement(@nospecialize(t::Type))
    t isa Union || return (isleaftype(t) || t === Record) ? t : Any
    u = Base.nonmissingtype(t)
    return (t === Union{Missing,u} && ((isleaftype(u) && u !== Missing) || u === Record)) ? t : Any
end

# A chain of comparisons rather than a loop, so that it is a constant where `t` is.
@eval isleaftype(@nospecialize(t)) = $(foldr((x, rest) -> :(t === $x || $rest), LEAF_TYPES; init=false))

function elementtype(@nospecialize(s::Schema))
    return narrowelement(juliatype(s))
end

"""
    nullablebranch(union) -> Int

For the two-branch nullable form (`["null", T]` / `[T, "null"]`) the position of the `null` branch;
0 otherwise.
"""
function nullablebranch(s::UnionSchema)
    length(s.branches) == 2 || return 0
    s.branches[1] isa NullSchema && !(s.branches[2] isa NullSchema) && return 1
    s.branches[2] isa NullSchema && !(s.branches[1] isa NullSchema) && return 2
    return 0
end
