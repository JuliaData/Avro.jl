# Typed decoding (plan §4.8) — implemented later in Phase 2.

function typedplan(::Type{T}, reader::Schema, plan::ReadPlan, limits::Limits) where {T}
    throw(ArgumentError("typed decoding into $T is not implemented yet"))
end
