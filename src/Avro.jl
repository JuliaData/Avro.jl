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

include("errors.jl")
include("frozen.jl")
include("limits.jl")
include("admission.jl")
include("values.jl")
include("names.jl")
include("jsonreader.jl")
include("logical.jl")

# `public` declarations parse only on Julia ≥ 1.11; evaluated through `Core.eval` so 1.10 still loads.
if VERSION >= v"1.11.0-DEV.469"
    Core.eval(@__MODULE__, Expr(:public,
        :AvroError, :DecodeError, :SchemaError, :EncodeError, :ResolutionError, :LimitError, :CodecError,
        :UnsupportedCodecError, :ConversionError, :UnknownSchemaError, :AmbiguousSchemaError,
        :WriterClosedError, :DataError,
        :Limits, :SymbolAdmission, :DEFAULT_ADMISSION,
        :Decimal, :WideDecimal, :Timestamp, :LocalTimestamp, :Duration, :truncate, :round))
end

end # module Avro
