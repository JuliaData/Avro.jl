# Fuzz gate (plan §9.7): the recorded sample (200 corpus entries × 1,000 seeded mutations) runs in
# sandboxed subprocess batches under CPU, RSS and wall-clock limits; every case must end in an AvroError
# or a valid result, and the acceptance-equivalence rules of §9.8 must hold. AVRO_FUZZ_ITERATIONS=N runs
# the whole corpus with N mutations per entry. Failures are shrunk and persisted under test/fuzz/failures.
include(joinpath(@__DIR__, "fuzz", "worker.jl"))

@testset "Fuzz: the recorded sample in sandboxed batches" begin
    sample = joinpath(@__DIR__, "fuzz", "sample.tsv")
    @test samplefile(fuzzsample(FIXTURES)) == samplefile(readsample(sample)) # the recorded sample is reproducible
    rng = Random.Xoshiro(1)
    ops = Set{Symbol}()
    for it in 1:300
        push!(ops, mutate(rng, UInt8[1, 2, 3, 4, 5], it)[2])
    end
    @test ops == Set([:truncate, :bitflip, :insert, :delete, :varint, :extreme, :overwrite])
    @test shrink(b -> 0x07 in b, UInt8[1, 2, 7, 3, 4, 5, 6, 8, 9]) == UInt8[7]
    @test shrink(b -> length(b) >= 3, collect(0x01:0x10)) |> length == 3
    for kind in (:datum, :schema, :json, :single)                               # one entry of each kind in-process
        e = first(filter(x -> x.kind === kind, fuzzcorpus(FIXTURES)))
        @test runentry(FIXTURES, e, 20, Avro.Limits(), mktempdir()).failures == 0
    end
    entries = readsample(sample)
    iterations = parse(Int, get(ENV, "AVRO_FUZZ_ITERATIONS", "1000"))
    if haskey(ENV, "AVRO_FUZZ_ITERATIONS")
        sample = tempname()
        write(sample, samplefile(fuzzcorpus(FIXTURES)))
        entries = readsample(sample)
    end
    batch = 25
    faildir = joinpath(@__DIR__, "fuzz", "failures")
    logdir = mktempdir()
    worker = joinpath(@__DIR__, "fuzz", "worker.jl")
    ranges = [i:min(i + batch - 1, length(entries)) for i in 1:batch:length(entries)]
    function launch(k)
        r = ranges[k]
        log = joinpath(logdir, "batch$k.log")
        cmd = `$(Base.julia_cmd()) --startup-file=no --code-coverage=none --track-allocation=none --project=$(Base.active_project()) $worker $FIXTURES $sample $(first(r)) $(last(r)) $iterations $(joinpath(logdir, "batch$k.tsv")) $faildir 4096 900`
        return run(pipeline(cmd; stdout=log, stderr=log); wait=false)
    end
    cases = 0
    failures = 0
    exits = Int[]
    for chunk in Iterators.partition(eachindex(ranges), 2)
        procs = [k => launch(k) for k in chunk]
        t0 = time()
        while any(process_running(p) for (_, p) in procs) && time() - t0 < 1200
            sleep(0.5)
        end
        for (k, p) in procs
            process_running(p) && (kill(p); wait(p))
            push!(exits, p.exitcode)
            out = joinpath(logdir, "batch$k.tsv")
            isfile(out) && for line in eachline(out)
                f = split(line, '\t')
                cases += parse(Int, f[5])
                failures += parse(Int, f[6])
            end
            p.exitcode == 0 || @warn "fuzz batch $k exited with $(p.exitcode)" log=read(joinpath(logdir, "batch$k.log"), String)
        end
    end
    @test all(==(0), exits)
    @test cases == length(entries) * iterations
    @test failures == 0
    failures == 0 || @warn "fuzz failures were shrunk and persisted" faildir
end
