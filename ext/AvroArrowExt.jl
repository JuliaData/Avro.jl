module AvroArrowExt

using Avro, Arrow, Dates
import Arrow.ArrowTypes

# Native Arrow timestamps retain the full Int64 tick range in every supported unit.
for (period, unit) in ((Millisecond, Arrow.Meta.TimeUnit.MILLISECOND),
                       (Microsecond, Arrow.Meta.TimeUnit.MICROSECOND),
                       (Nanosecond, Arrow.Meta.TimeUnit.NANOSECOND))
    for (wrapper, zone, name) in ((Avro.Timestamp, :UTC, Symbol("JuliaLang.Avro.Timestamp")),
                                   (Avro.LocalTimestamp, nothing, Symbol("JuliaLang.Avro.LocalTimestamp")))
        T = wrapper{period}
        A = Arrow.Timestamp{unit,zone}
        @eval begin
            ArrowTypes.ArrowType(::Type{$T}) = $A
            ArrowTypes.toarrow(x::$T) = $A(x.ticks)
            ArrowTypes.arrowname(::Type{$T}) = $(QuoteNode(name))
            ArrowTypes.JuliaType(::Val{$(QuoteNode(name))}, ::Type{$A}) = $T
            ArrowTypes.fromarrow(::Type{$T}, x::$A) = $T(x.x)
            ArrowTypes.default(::Type{$T}) = $T(0)
        end
    end
end

end
