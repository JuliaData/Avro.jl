"""
    Avro

A production-grade implementation of the Apache Avro data format (schemas, binary and JSON encodings,
schema resolution, single-object encoding, object container files) for Julia, with Tables.jl
integration. The API is fully qualified: nothing is exported.

See `AVRO_REWRITE_PLAN.md` for the design and `STATUS.md` for the implementation status.
"""
module Avro

using Dates
using UUIDs
using Tables: Tables
import MD5
import SHA

include("errors.jl")
include("frozen.jl")
include("limits.jl")
include("admission.jl")
include("values.jl")
include("names.jl")
include("jsonreader.jl")
include("logical.jl")
include("schema.jl")
include("canonical.jl")
include("generic.jl")
include("storage.jl")
include("types.jl")
include("decoder.jl")
include("encoder.jl")
include("plan_read.jl")
include("plan_write.jl")
include("columns.jl")
include("resolution.jl")
include("typed.jl")
include("prepared.jl")
include("jsonencoding.jl")
include("singleobject.jl")

# `public` declarations parse only on Julia ≥ 1.11; evaluated through `Core.eval` so 1.10 still loads.
if VERSION >= v"1.11.0-DEV.469"
    Core.eval(@__MODULE__, Expr(:public,
        :AvroError, :DecodeError, :SchemaError, :EncodeError, :ResolutionError, :LimitError, :CodecError,
        :UnsupportedCodecError, :ConversionError, :UnknownSchemaError, :AmbiguousSchemaError,
        :WriterClosedError, :DataError,
        :Limits, :SymbolAdmission, :DEFAULT_ADMISSION,
        :Decimal, :WideDecimal, :Timestamp, :LocalTimestamp, :Duration, :truncate, :round,
        :Schema, :parseschema, :json, :canonical, :fingerprint, :parsingequivalent, :fullname, :nodefault,
        :NullSchema, :BooleanSchema, :IntSchema, :LongSchema, :FloatSchema, :DoubleSchema, :BytesSchema, :StringSchema,
        :ArraySchema, :MapSchema, :UnionSchema, :RecordSchema, :EnumSchema, :FixedSchema, :Field,
        :DecimalLogical, :UUIDLogical, :DateLogical, :TimeMillis, :TimeMicros, :TimestampMillis, :TimestampMicros,
        :TimestampNanos, :LocalTimestampMillis, :LocalTimestampMicros, :LocalTimestampNanos, :DurationLogical, :UnknownLogical, :Map, :Record, :EnumValue, :Fixed, :UnionValue, :ordinal, :juliatype, :valuetypes, :minsize, :schema, :avroname, :avrosymbol, :AvroStyle,
        :DatumReader, :DatumWriter, :encode, :encode!, :decode, :tojson, :fromjson,
        :SchemaStore, :SchemaCache, :register!, :lookup, :encodesingle, :decodesingle))
end

function __init__()
    STORAGE[] = measurestorage()
    return nothing
end

end # module Avro
