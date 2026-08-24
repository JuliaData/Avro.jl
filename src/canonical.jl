# Parsing Canonical Form and fingerprints (spec "Parsing Canonical Form for Schemas").

"""
    Avro.canonical(schema; limits=Limits()) -> String

The Parsing Canonical Form of `schema`: primitives as bare strings, named types by fullname with
`name`/`type`/`fields`/`symbols`/`items`/`values`/`size` in spec order, the first definition in full and
later references by fullname, no whitespace, no attributes other than the canonical ones. A graph with
repaired invalid names has no specification PCF and raises `ArgumentError`.
"""
function canonical(s::Schema; limits::Limits=graphlimits(s))
    graphinfo(s).repaired_names && throw(ArgumentError("a schema with repaired invalid names has no Parsing Canonical Form"))
    return withbudget(limits) do budget
        w = BoundedWriter(budget, limits.max_schema_bytes)
        canonicalprint(w, s, FrozenDict{String,Bool}())
        return String(take!(w.io))
    end
end

function canonicalprint(io::IO, s::Schema, seen::FrozenDict{String,Bool})
    if s isa PrimitiveSchema
        print(io, '"', kind(s), '"')
    elseif s isa UnionSchema
        print(io, '[')
        for (i, b) in enumerate(s.branches)
            i > 1 && print(io, ',')
            canonicalprint(io, b, seen)
        end
        print(io, ']')
    elseif s isa ArraySchema
        print(io, "{\"type\":\"array\",\"items\":")
        canonicalprint(io, s.items, seen)
        print(io, '}')
    elseif s isa MapSchema
        print(io, "{\"type\":\"map\",\"values\":")
        canonicalprint(io, s.values, seen)
        print(io, '}')
    else
        full = fullname(s)
        if haskey(seen, full)
            escapejson(io, full)
            countnode!(io)
            return nothing
        end
        seen[full] = true
        chargeseen!(io, 32 + sizeof(full))
        print(io, "{\"name\":")
        escapejson(io, full)
        if s isa FixedSchema
            print(io, ",\"type\":\"fixed\",\"size\":", s.size, '}')
        elseif s isa EnumSchema
            print(io, ",\"type\":\"enum\",\"symbols\":[")
            for (i, sym) in enumerate(s.symbols)
                i > 1 && print(io, ',')
                escapejson(io, sym)
            end
            print(io, "]}")
        else
            print(io, ",\"type\":\"record\",\"fields\":[")
            for (i, f) in enumerate(s.fields)
                i > 1 && print(io, ',')
                print(io, "{\"name\":")
                escapejson(io, f.name)
                print(io, ",\"type\":")
                canonicalprint(io, f.schema, seen)
                print(io, '}')
            end
            print(io, "]}")
        end
    end
    countnode!(io)
    return nothing
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

crc64avro(s::AbstractString) = crc64avro(codeunits(s))

"""
    Avro.fingerprint(schema; algorithm=:crc64avro, limits=Limits()) -> UInt64 | Vector{UInt8}

A fingerprint of the Parsing Canonical Form: `:crc64avro` (`UInt64`), `:md5` or `:sha256` (bytes).
"""
function fingerprint(s::Schema; algorithm::Symbol=:crc64avro, limits::Limits=graphlimits(s))
    pcf = canonical(s; limits=limits)
    algorithm === :crc64avro && return crc64avro(pcf)
    algorithm === :md5 && return MD5.md5(pcf)
    algorithm === :sha256 && return SHA.sha256(pcf)
    throw(ArgumentError("unknown fingerprint algorithm :$algorithm (use :crc64avro, :md5 or :sha256)"))
end

"""
    Avro.parsingequivalent(a, b; limits=Limits()) -> Bool

`true` when the two schemas have the same Parsing Canonical Form.
"""
parsingequivalent(a::Schema, b::Schema; limits::Limits=Limits()) = canonical(a; limits=limits) == canonical(b; limits=limits)
