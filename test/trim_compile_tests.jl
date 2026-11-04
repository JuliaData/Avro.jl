# Builds test/trim/workload.jl with `juliac --trim=safe` and runs the executable on one thread and on two. The
# verifier must report no errors or warnings (its summary line appears only when there are some). Opt-in: run
# this file directly.
using Test

# Julia 1.13 and earlier can write a trimmed image whose shared empty `Memory{Any}` is overwritten, depending on
# which method specializations it keeps (JuliaLang/julia#63664, fixed in 1.14). The executable then crashes in
# its first dynamic dispatch, before `main`; on one thread that is the main thread, which prints this stack.
knownjuliabug(output) = VERSION < v"1.14.0-" && occursin("jl_egal_", output) && occursin("lookup_leafcache", output)

function runexe(exe, dir, threads)
    log = joinpath(dir, "run-$threads.log")
    ok = success(pipeline(Cmd(addenv(`$exe`, "JULIA_NUM_THREADS" => threads); dir=dir); stdout=log, stderr=log))
    return (ok=ok, output=read(log, String))
end

function trimtest()
    project = joinpath(@__DIR__, "trim")
    julia = Base.julia_cmd()
    run(`$julia --startup-file=no --project=$project -e "using Pkg; Pkg.instantiate()"`)
    dir = mktempdir()
    log = joinpath(dir, "build.log")
    name = Sys.iswindows() ? "avrotrim.exe" : "avrotrim"
    build = `$julia --startup-file=no --project=$project -e "using JuliaC; JuliaC.main(ARGS)" --
             --output-exe avrotrim --project=$project --experimental --trim=safe $(joinpath(project, "workload.jl"))`
    built = success(pipeline(Cmd(build; dir=dir); stdout=log, stderr=log))
    output = read(log, String)
    (built && !occursin("Trim verify finished", output)) || println(output)
    @test built
    @test !occursin("Trim verify finished", output)
    built || return
    single = runexe(joinpath(dir, name), dir, "1,0")
    if !single.ok && knownjuliabug(single.output)
        @warn "the trimmed executable hit JuliaLang/julia#63664 (fixed in Julia 1.14)"
        @test_broken single.ok
        return
    end
    for r in (single, runexe(joinpath(dir, name), dir, "2"))
        r.ok && chomp(r.output) == "ok" || println(r.output)
        @test r.ok && chomp(r.output) == "ok"
    end
end

@testset "juliac --trim=safe" begin
    trimtest()
end
