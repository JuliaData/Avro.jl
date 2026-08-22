# Schema resolution (plan §4.7) — implemented in Phase 3.

"""
    resolvingplan(writer, reader; union_resolution=:spec, limits=Limits()) -> ReadPlan

A read plan that decodes data written with `writer` into values of `reader`.
"""
function resolvingplan(writer::Schema, reader::Schema; union_resolution::Symbol=:spec, limits::Limits=Limits())
    union_resolution in (:spec, :java) || throw(ArgumentError("union_resolution must be :spec or :java"))
    throw(ArgumentError("schema resolution is not implemented yet (Phase 3)"))
end
