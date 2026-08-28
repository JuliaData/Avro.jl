# Cross-version consumer: verifies every file written by write.jl on another Julia version.
using Avro, Test
indir = abspath(ARGS[1])
writer = strip(read(joinpath(indir, "VERSION"), String))
@testset "cross-version read (written by Julia $writer, read by $VERSION)" begin
    for jsonl in filter(f -> endswith(f, ".jsonl"), readdir(indir; join=true))
        name = basename(jsonl)[1:end - 6]
        expected = filter(!isempty, readlines(jsonl))
        for codec in ("null", "deflate", "zstandard", "snappy")
            file = joinpath(indir, "$name-$codec.avro")
            Avro.Reader(file) do r
                s = Avro.writerschema(r)
                vals = collect(Avro.eachdatum(r))
                @test length(vals) == length(expected)
                @test all(isequal(Avro.fromjson(s, expected[i]), vals[i]) for i in eachindex(vals))
            end
        end
    end
end
println("verified from Julia ", VERSION)
