# The §10.2 performance gates under the §10.1 measurement protocol (opt-in: AVRO_PERF=true on the
# named authoring host). Every gated number is the median of 5 cold processes. Workload metrics use
# the best of 3 in-process repetitions after one warm-up (test/perf/cold.jl); load and time-to-first-
# table measure cold startup directly. Ratio gates compare against the Avro.jl 1.1.2 baselines
# measured on the same host and Julia (recorded below); every numeric §10.2 target is enforced on the
# named host.

@testset "Performance gates (plan §10.1/§10.2)" begin
    BASE_WRITE_112 = 0.9460       # Avro 1.1.2 writetable, §10.1 protocol: median of 5 cold processes,
    BASE_READMAT_112 = 5.0513     # each best-of-3, deterministic {id,x,name,flag} data — recorded with
                                  # the full lines in benchmarks/logs/avro112.log (2026-08-24)
    cold = joinpath(@__DIR__, "perf", "cold.jl")
    project = Base.active_project()
    # Pkg.test forces --check-bounds=yes on this process and Base.julia_cmd() reproduces it; the
    # §10.1 protocol measures default-flag cold processes (the 1.1.2 baselines were recorded that
    # way), so the child drops the flag — globally-forced bounds checks compress the projection
    # skip-path ratio from ≈2.4 to ≈1.85 while the suite's own assertions keep running checked.
    juliacmd = Cmd(filter(a -> !startswith(a, "--check-bounds"), Base.julia_cmd().exec))
    function coldmetric(metric; threads=1)
        samples = Vector{Float64}[]
        for _ in 1:5
            out = read(`$juliacmd --startup-file=no --threads=$threads --project=$project $cold $metric`, String)
            fields = split(strip(out))
            @assert fields[1] == metric
            push!(samples, parse.(Float64, fields[2:end]))
        end
        width = length(first(samples))
        @assert width > 0 && all(length(sample) == width for sample in samples)
        return ntuple(width) do i
            values = sort!([sample[i] for sample in samples])
            values[3]
        end
    end
    twrite = only(coldmetric("write"))
    @test twrite <= BASE_WRITE_112 / 4                              # ≥ 4× vs 1.1.2
    @test twrite <= 0.25                                            # ≥ 80 MB/s on the named host
    ttable = only(coldmetric("table1"))
    @test ttable <= BASE_READMAT_112 / 10                           # ≥ 10× vs 1.1.2 read + materialise
    @test ttable <= 0.30
    ratio8 = nothing
    if Sys.CPU_THREADS >= 8
        ratio8 = only(coldmetric("table8"; threads=8))
        @test ratio8 >= 3                                           # 8 tasks vs 1, 4 GiB limits on both
    end
    overheads = Dict{String,Float64}()
    for codec in ("zstandard", "deflate", "snappy")
        o = only(coldmetric("codec-$codec"))
        overheads[codec] = o
        @test o <= 1.3                                              # ≤ 1.3 × (null table + isolated transcode)
    end
    projratio = only(coldmetric("projection"))
    @test projratio >= 2                                            # select=(:id,) under :fast
    decallocs, dns, ea, ens, onedec, oneenc, parseus, parseallocs = coldmetric("kernels")
    @test decallocs <= 1                                            # prepared typed decode: the string only
    @test dns <= 150
    @test ea == 0                                                   # prepared typed encode: zero allocations
    @test parseus <= 100
    # Disputed (round-2 D07 sub-item): §10.2's gate column for `parseschema` is "—" — the ≤ 300
    # allocations figure sits in the informational column. Reaching it needs an arena-style parser
    # rewrite (the profile: ~287 boxed Ints, ~138 heap name tuples, per-token Strings), which is out of
    # proportion for an informational number this late. The assertion below is a calibrated regression
    # bound so parser-allocation regressions still fail loudly: measured 2,062 before the round-4
    # exact-capacity contract, 2,254 after it (prebuilt containers and replacement growth cost a few
    # vectors per node; shared frozen empties reclaim the alias-free and prop-free cases).
    @test parseallocs <= 2300
    tload = only(coldmetric("load"))
    @test tload <= 0.5
    tttft = only(coldmetric("ttft"))
    @test tttft <= 1.5
    @info "§10.1 medians (5 cold processes)" write_s = round(twrite; digits=3) write_ratio_vs_112 = round(BASE_WRITE_112 / twrite; digits=1) table1_s = round(ttable; digits=3) table_ratio_vs_112 = round(BASE_READMAT_112 / ttable; digits=1) ratio_8t = ratio8 === nothing ? "n/a" : round(ratio8; digits=2) codec_overheads = join(("$k=$(round(v; digits=2))" for (k, v) in overheads), " ") projection_ratio = round(projratio; digits=1) prepared_decode = "$(decallocs) allocs, $(round(dns; digits=0)) ns" prepared_encode = "$(ea) allocs, $(round(ens; digits=0)) ns" oneshot = "dec $(round(onedec; digits=0)) ns, enc $(round(oneenc; digits=0)) ns" parseschema = "$(round(parseus; digits=1)) μs, $(round(parseallocs; digits=0)) allocs" load_s = round(tload; digits=2) ttft_s = round(tttft; digits=2)
end
