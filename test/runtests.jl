using Test
using Avro
using Dates
using UUIDs
using StructUtils
using Tables
using Random

const FIXTURES = joinpath(@__DIR__, "fixtures")
include("valuegen.jl")

@testset "Avro" begin
    include("limits.jl")
    include("frozen.jl")
    include("admission.jl")
    include("values.jl")
    include("names.jl")
    include("json.jl")
    include("schema.jl")
    include("types.jl")
    include("binary.jl")
    include("storage.jl")
    include("typed.jl")
    include("jsonencoding.jl")
    include("singleobject.jl")
    include("columns.jl")
    include("legacy.jl")
    include("gates.jl")
    include("latency.jl")
    include("fuzz.jl")
    if get(ENV, "AVRO_QUALITY_GATES", "false") == "true"
        include("quality.jl")
    end
    if get(ENV, "AVRO_INTEROP", "false") == "true"
        include("interop.jl")
    end
end
