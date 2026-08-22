using Test
using Avro
using Dates
using UUIDs
using StructUtils
using Tables
using Random

const FIXTURES = joinpath(@__DIR__, "fixtures")

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
    include("typed.jl")
    include("jsonencoding.jl")
    include("singleobject.jl")
    if get(ENV, "AVRO_QUALITY_GATES", "false") == "true"
        include("quality.jl")
    end
end
