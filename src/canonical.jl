# Parsing Canonical Form and fingerprints (spec "Parsing Canonical Form for Schemas").

"""
    Avro.canonical(schema) -> String

The Parsing Canonical Form of `schema`: primitives as bare strings, named types by fullname with
`name`/`type`/`fields`/`symbols`/`items`/`values`/`size` in spec order, the first definition in full and
later references by fullname, no whitespace, no attributes other than the canonical ones. A graph with
repaired invalid names has no specification PCF and raises `ArgumentError`.
"""
function canonical(s::Schema)
    return String(canonicalbytes(s))
end

function canonicalbytes(s::Schema)
    graphinfo(s).repaired_names && throw(ArgumentError("a schema with repaired invalid names has no Parsing Canonical Form"))
    return pcfbytes(s)
end

function pcfbytes(s::Schema)
    io = IOBuffer()
    printjson(io, canonicaltree(s, schemaseen(s)), false, 0)
    return take!(io)
end

function canonicaltree(s::Schema, seen::Vector{Bool})
    s isa PrimitiveSchema && return String(kind(s))
    s isa UnionSchema && return Any[canonicaltree(b, seen) for b in s.branches]
    s isa ArraySchema && return Pair{String,Any}["type" => "array", "items" => canonicaltree(s.items, seen)]
    s isa MapSchema && return Pair{String,Any}["type" => "map", "values" => canonicaltree(s.values, seen)]
    index = Int(nodeid(s)) + 1
    seen[index] && return fullname(s)
    seen[index] = true
    s isa FixedSchema && return Pair{String,Any}["name" => fullname(s), "type" => "fixed", "size" => Int64(s.size)]
    s isa EnumSchema && return Pair{String,Any}["name" => fullname(s), "type" => "enum", "symbols" => s.symbols]
    fields = Any[Pair{String,Any}["name" => f.name, "type" => canonicaltree(f.schema, seen)] for f in s.fields]
    return Pair{String,Any}["name" => fullname(s), "type" => "record", "fields" => fields]
end

# CRC-64-AVRO (spec pseudo-code): empty = 0xc15d213aa4d7a795, table built from the polynomial.
const CRC64_EMPTY = 0xc15d213aa4d7a795

function crc64table()
    table = Vector{UInt64}(undef, 256)
    for i in 0:255
        fp = UInt64(i)
        for _ in 1:8
            fp = (fp >> 1) ⊻ (CRC64_EMPTY & -(fp & 1))
        end
        table[i + 1] = fp
    end
    return table
end

const CRC64_TABLE = crc64table()

"""
    crc64avro(bytes) -> UInt64

The CRC-64-AVRO fingerprint of `bytes` (spec pseudo-code).
"""
function crc64avro(bytes::AbstractVector{UInt8})
    fp = CRC64_EMPTY
    for b in bytes
        fp = (fp >> 8) ⊻ CRC64_TABLE[((fp ⊻ b) & 0xff) + 1]
    end
    return fp
end

function crc64avro(s::AbstractString)
    return crc64avro(codeunits(s))
end

"""
    Avro.fingerprint(schema; algorithm=:crc64avro) -> UInt64 | Vector{UInt8}

A fingerprint of the Parsing Canonical Form: `:crc64avro` (`UInt64`), `:md5` or `:sha256` (bytes).
"""
function fingerprint(s::Schema; algorithm::Symbol=:crc64avro)
    algorithm in (:crc64avro, :md5, :sha256) ||
        throw(ArgumentError("unknown fingerprint algorithm :$algorithm (use :crc64avro, :md5 or :sha256)"))
    pcf = canonicalbytes(s)
    algorithm === :crc64avro && return crc64avro(pcf)
    return algorithm === :md5 ? MD5.md5(pcf) : SHA.sha256(pcf)
end

"""
    Avro.parsingequivalent(a, b) -> Bool

`true` when the two schemas have the same Parsing Canonical Form.
"""
function parsingequivalent(a::Schema, b::Schema)
    return canonicalbytes(a) == canonicalbytes(b)
end
