# Parsing Canonical Form and fingerprints (spec "Parsing Canonical Form for Schemas").

"""
    Avro.canonical(schema; limits=Limits()) -> String

The Parsing Canonical Form of `schema`: primitives as bare strings, named types by fullname with
`name`/`type`/`fields`/`symbols`/`items`/`values`/`size` in spec order, the first definition in full and
later references by fullname, no whitespace, no attributes other than the canonical ones. A graph with
repaired invalid names has no specification PCF and raises `ArgumentError`.
"""
function canonical(s::Schema; limits::Limits=Limits())
    graphinfo(s).repaired_names && throw(ArgumentError("a schema with repaired invalid names has no Parsing Canonical Form"))
    return withbudget(limits) do budget
        w = BoundedWriter(budget, limits.max_schema_bytes)
        seen = schemaseen(s, budget)
        canonicalprint(w, s, seen)
        releaseseen!(budget, seen)
        return boundedtake!(w)
    end
end

function canonicalprint(io::IO, s::Schema, seen::Vector{Bool})
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
        seenindex = Int(nodeid(s)) + 1
        if seen[seenindex]
            escapefullname(io, s)
            countnode!(io)
            return nothing
        end
        seen[seenindex] = true
        print(io, "{\"name\":")
        escapefullname(io, s)
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

function crc64avro(s::AbstractString)
    return crc64avro(codeunits(s))
end

"Hash canonical bytes after reserving the exact retained digest and pinned-library workspace."
function hashfingerprint(pcf::AbstractVector{UInt8}, algorithm::Symbol, budget::Budget)
    # MD5 retains a 16-byte state vector behind a 16-byte ReinterpretArray shell. SHA-256
    # retains its 32-byte output vector. Their pinned implementations also allocate one
    # context object, one state vector and one 64-byte block buffer while hashing.
    retained = algorithm === :md5 ? checked_add(bytesbytes(16), 16) : bytesbytes(32)
    transient = algorithm === :md5 ? checked_add(32, bytesbytes(64)) :
                checked_add(32, checked_add(bytesbytes(32), bytesbytes(64)))
    peak = checked_add(retained, transient)
    reserve!(budget, peak)
    try
        digest = algorithm === :md5 ? MD5.md5(pcf) : SHA.sha256(pcf)
        allocated!(budget, peak)
        release!(budget, transient)
        return digest
    catch
        unreserve!(budget, peak)
        rethrow()
    end
end

"""
    Avro.fingerprint(schema; algorithm=:crc64avro, limits=Limits()) -> UInt64 | Vector{UInt8}

A fingerprint of the Parsing Canonical Form: `:crc64avro` (`UInt64`), `:md5` or `:sha256` (bytes).
"""
function fingerprint(s::Schema; algorithm::Symbol=:crc64avro, limits::Limits=Limits())
    graphinfo(s).repaired_names && throw(ArgumentError("a schema with repaired invalid names has no Parsing Canonical Form"))
    algorithm in (:crc64avro, :md5, :sha256) ||
        throw(ArgumentError("unknown fingerprint algorithm :$algorithm (use :crc64avro, :md5 or :sha256)"))
    return withbudget(limits) do budget                # one operation: print and hash in the same scope (D02)
        w = BoundedWriter(budget, limits.max_schema_bytes)
        seen = schemaseen(s, budget)
        canonicalprint(w, s, seen)
        releaseseen!(budget, seen)                     # the seen table dies before hashing (round-3 item 1)
        pcf = boundedview(w)
        algorithm === :crc64avro && return crc64avro(pcf)
        return hashfingerprint(pcf, algorithm, budget)
    end
end

"""
    Avro.parsingequivalent(a, b; limits=Limits()) -> Bool

`true` when the two schemas have the same Parsing Canonical Form.
"""
function parsingequivalent(a::Schema, b::Schema; limits::Limits=Limits())
    for s in (a, b)
        graphinfo(s).repaired_names && throw(ArgumentError("a schema with repaired invalid names has no Parsing Canonical Form"))
    end
    return withbudget(limits) do budget                # one operation for both canonical forms (D02)
        wa = BoundedWriter(budget, limits.max_schema_bytes)
        seena = schemaseen(a, budget)
        canonicalprint(wa, a, seena)
        releaseseen!(budget, seena)                    # dead before the second print (round-3 item 1)
        wb = BoundedWriter(budget, limits.max_schema_bytes)
        seenb = schemaseen(b, budget)
        canonicalprint(wb, b, seenb)
        releaseseen!(budget, seenb)
        return boundedview(wa) == boundedview(wb)
    end
end
