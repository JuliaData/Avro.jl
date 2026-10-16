# Informational: trim-compile the reader program (juliac is experimental; failures do not gate).
juliac = joinpath(Sys.BINDIR, Base.DATAROOTDIR, "julia", "juliac.jl")
isfile(juliac) || (println("juliac.jl not found; skipping"); exit(0))
target = joinpath(@__DIR__, "reader.jl")
out = joinpath(mktempdir(), "avroreader")
cmd = `$(Base.julia_cmd()) --project=$(dirname(dirname(@__DIR__))) $juliac --experimental --trim=safe --output-exe $out $target`
println("running: ", cmd)
ok = success(run(ignorestatus(cmd)))
println(ok ? "trim compile succeeded" : "trim compile failed (informational)")
