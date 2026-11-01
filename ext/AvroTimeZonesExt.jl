# TimeZones.jl integration: a `ZonedDateTime` is an instant of millisecond resolution, so it writes to the
# Avro `timestamp-*` logical types and reads back from them in UTC.
module AvroTimeZonesExt

using Avro: Avro
using Dates: DateTime, UTC
using Durations: ZonedTimestamp
using StructUtils: StructUtils
using TimeZones: ZonedDateTime

function Avro.deriveimpl(::Avro.DeriveContext, ::Type{ZonedDateTime}, name, namespace)
    return Avro.logicalprimitive(Avro.LongSchema, Avro.TimestampMillis())
end

function Avro.encodevalue(p::Avro.TimestampPlan, e::Avro.Encoder, x::ZonedDateTime)
    return Avro.encodevalue(p, e, DateTime(x, UTC))
end

Avro.accepts(::Avro.TimestampPlan, ::ZonedDateTime) = true

StructUtils.structlike(::Avro.AvroStyle, ::Type{ZonedDateTime}) = false

function StructUtils.lift(st::Avro.AvroStyle, ::Type{ZonedDateTime}, x::ZonedTimestamp)
    return ZonedDateTime(x), StructUtils.defaultstate(st)
end

end
