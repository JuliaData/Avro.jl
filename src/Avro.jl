"""
    Avro

A production-grade implementation of the Apache Avro data format (schemas, binary and JSON encodings,
schema resolution, single-object encoding, object container files) for Julia, with Tables.jl
integration. The API is fully qualified: nothing is exported.

See `AVRO_REWRITE_PLAN.md` for the design and `STATUS.md` for the implementation status.
"""
module Avro

import DataDecimals, Durations

using Dates
using UUIDs
using Random: RandomDevice, rand
using Tables: Tables
import MD5
import SHA
import JSON

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
include("span.jl")
include("plan_write.jl")
include("columns.jl")
include("resolution.jl")
include("compare.jl")
include("typed.jl")
include("prepared.jl")
include("jsonencoding.jl")
include("singleobject.jl")
include("codecs.jl")
include("container.jl")
include("parallel.jl")
include("tables.jl")
include("deprecated.jl")

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
        :SchemaStore, :SchemaCache, :register!, :lookup, :encodesingle, :decodesingle,
        :resolve, :ResolvedSchema, :compare, :comparebytes,
        :Reader, :Writer, :write, :tobuffer, :codecs, :inspect, :InspectReport,
        :metadata, :codec, :sync, :writerschema, :eachblock, :eachdatum,
        :Table, :Rows, :Row, :record,
        :read, :readtable, :writetable))
end

function __init__()
    STORAGE[] = measurestorage()
    return nothing
end

"`JSON.json` prints an `Avro.Schema` as its schema JSON (parsing is Avro-owned; plan §11)."
function JSON.json(s::Schema)
    return json(s)
end

function JSON.json(io::IO, s::Schema)
    return (Base.write(io, json(s)); nothing)
end

include("precompile.jl")
end # module Avro
