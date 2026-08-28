# Error hierarchy (plan §4.13). Every defect the package detects in its input surfaces as an `AvroError`;
# exceptions that are not about the content (IOError, InterruptException, OutOfMemoryError, user-hook
# errors) propagate unchanged.

"""
    Avro.AvroError <: Exception

Abstract supertype of every error Avro.jl raises about the content it processes (schemas, datums,
container files, JSON, limits, codecs). I/O errors, interruptions and errors thrown by user code are
never wrapped.
"""
abstract type AvroError <: Exception end

"""
    Avro.DecodeError <: AvroError

Abstract supertype of errors detected while decoding bytes.
"""
abstract type DecodeError <: AvroError end

"""
    Avro.SchemaError(msg, path)

A schema document violates the Avro schema rules (plan §3/§4.2). `path` is the JSON path of the offending
element (e.g. `"\$.fields[2].type"`).
"""
struct SchemaError <: AvroError
    msg::String
    path::String
end

"""
    Avro.EncodeError(msg, path, schema)

A Julia value cannot be encoded under `schema`; `path` locates the value (e.g. `"\$.items[3]"`).
"""
struct EncodeError <: AvroError
    msg::String
    path::String
    schema::Any
end

"""
    Avro.ResolutionError(msg, writerpath, readerpath)

The writer and reader schemas do not resolve (plan §4.7).
"""
struct ResolutionError <: AvroError
    msg::String
    writerpath::String
    readerpath::String
end

"""
    Avro.LimitError(limit, observed, value, keyword; direction)

A configured resource limit was exceeded. `limit` names the limit, `observed` is the value that tripped
it, `value` the configured bound, and `keyword` the `Avro.Limits` keyword that raises it (the manual
states that limits must be raised on every side that processes the data).
"""
struct LimitError <: AvroError
    limit::Symbol
    observed::Int
    value::Int
    keyword::Symbol
    direction::Symbol   # :decode | :encode
end

function LimitError(limit::Symbol, observed::Integer, value::Integer; direction::Symbol=:decode)
    return LimitError(limit, Int(observed), Int(value), limit, direction)
end

"""
    Avro.CodecError(codec, direction, msg)

A compressed payload could not be processed (`direction` is `:compress` or `:decompress`).
"""
struct CodecError <: AvroError
    codec::Symbol
    direction::Symbol
    msg::String
end

"""
    Avro.UnsupportedCodecError(codec, package)

The container names a codec this process cannot handle; `package` names the extension package to load
(`nothing` when the codec is unknown to Avro.jl).
"""
struct UnsupportedCodecError <: AvroError
    codec::String
    package::Union{Nothing,String}
end

"""
    Avro.ConversionError(msg)

A lossy or out-of-range native conversion (e.g. a timestamp that does not fit a `DateTime`).
"""
struct ConversionError <: AvroError
    msg::String
end

"""
    Avro.UnknownSchemaError(fingerprint)

A single-object message references a fingerprint the schema store does not contain.
"""
struct UnknownSchemaError <: AvroError
    fingerprint::UInt64
end

"""
    Avro.AmbiguousSchemaError(fingerprint, existing, offered)

A schema store already holds a different schema under the same fingerprint.
"""
struct AmbiguousSchemaError <: AvroError
    fingerprint::UInt64
    existing::Any
    offered::Any
end

"""
    Avro.WriterClosedError(cause)

The writer was closed, or poisoned by an earlier failure (`cause` carries the original exception).
"""
struct WriterClosedError <: AvroError
    cause::Union{Nothing,Exception}
end

"""
    Avro.DataError(msg, pos, path)

Malformed encoded input at byte position `pos` (1-based into the buffer being decoded; 0 when unknown);
`path` optionally locates the value.
"""
struct DataError <: DecodeError
    msg::String
    pos::Int
    path::String
end

function DataError(msg::AbstractString, pos::Integer=0)
    return DataError(String(msg), Int(pos), "")
end

function Base.showerror(io::IO, e::SchemaError)
    print(io, "SchemaError: ", e.msg)
    isempty(e.path) || print(io, " (at ", e.path, ")")
    return nothing
end

function Base.showerror(io::IO, e::EncodeError)
    print(io, "EncodeError: ", e.msg)
    isempty(e.path) || print(io, " (at ", e.path, ")")
    return nothing
end

function Base.showerror(io::IO, e::ResolutionError)
    print(io, "ResolutionError: ", e.msg, " (writer ", e.writerpath, ", reader ", e.readerpath, ")")
    return nothing
end

function Base.showerror(io::IO, e::LimitError)
    print(io, "LimitError: ", e.limit, " exceeded while ", e.direction == :encode ? "encoding" : "decoding",
        ": observed ", e.observed, ", limit ", e.value, "; raise `Avro.Limits(", e.keyword, "=...)` on every side that processes this data")
    return nothing
end

function Base.showerror(io::IO, e::CodecError)
    print(io, "CodecError: ", e.codec, " ", e.direction, ": ", e.msg)
    return nothing
end

function Base.showerror(io::IO, e::UnsupportedCodecError)
    print(io, "UnsupportedCodecError: codec \"", e.codec, "\"")
    e.package === nothing || print(io, " requires `using ", e.package, "`")
    return nothing
end

function Base.showerror(io::IO, e::ConversionError)
    print(io, "ConversionError: ", e.msg)
    return nothing
end

function Base.showerror(io::IO, e::UnknownSchemaError)
    print(io, "UnknownSchemaError: no schema registered for fingerprint 0x", string(e.fingerprint; base=16, pad=16))
    return nothing
end

function Base.showerror(io::IO, e::AmbiguousSchemaError)
    print(io, "AmbiguousSchemaError: a different schema is already registered under fingerprint 0x",
        string(e.fingerprint; base=16, pad=16))
    return nothing
end

function Base.showerror(io::IO, e::WriterClosedError)
    print(io, "WriterClosedError: the writer is closed")
    e.cause === nothing || print(io, " (poisoned by an earlier failure: ", sprint(showerror, e.cause), ")")
    return nothing
end

function Base.showerror(io::IO, e::DataError)
    print(io, "DataError: ", e.msg)
    e.pos > 0 && print(io, " (at byte ", e.pos, ")")
    isempty(e.path) || print(io, " (at ", e.path, ")")
    return nothing
end
