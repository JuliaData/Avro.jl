using Test
using Avro
using Dates
using UUIDs
using StructUtils
using Tables
using Random
import DataDecimals

const FIXTURES = joinpath(@__DIR__, "fixtures")
include("valuegen.jl")

include("sharedvalues.jl")

@testset "Avro" begin
    include("limits.jl")
    include("values.jl")
    include("names.jl")
    include("json.jl")
    include("schema.jl")
    include("types.jl")
    include("binary.jl")
    include("typed.jl")
    include("jsonencoding.jl")
    include("singleobject.jl")
    include("bytesources.jl")
    include("arrow.jl")
    include("codecs.jl")
    include("container.jl")
    include("tables.jl")
    include("interfaces.jl")
    include("nullable_write.jl")
    include("wide_rows.jl")
    include("invariant.jl")
    include("parallel.jl")
    include("projection.jl")
    include("deprecated.jl")
    include("documentation.jl")
    include("resolution.jl")
    include("compare.jl")
    include("columns.jl")
    include("legacy.jl")
    include("gates.jl")
    if get(ENV, "AVRO_SKIP_LATENCY", "false") != "true"
        include("latency.jl")
    end
    include("hostile.jl")
    include("fuzz.jl")
    if get(ENV, "AVRO_QUALITY_GATES", "false") == "true"
        include("quality.jl")
    end
    if get(ENV, "AVRO_INTEROP", "false") == "true"
        include("interop.jl")
    end
    if get(ENV, "AVRO_PERF", "false") == "true"
        include("perf.jl")
    end
    if get(ENV, "AVRO_SMOKE", "false") == "true"
        include("smoke.jl")
    end
end
