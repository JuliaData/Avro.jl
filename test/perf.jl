# The §10.2 performance gates under the §10.1 measurement protocol (opt-in: AVRO_PERF=true on the
# named authoring host). Every gated number is the median of 5 cold processes, each the best of 3
# in-process repetitions after a warm-up (test/perf/cold.jl). Ratio gates compare against the Avro.jl
# 1.1.2 baselines measured on the same host and Julia (recorded below); the plan's informational
# absolutes are logged and lightly sanity-asserted.

@testset "Performance gates (plan §10.1/§10.2)" begin
    BASE_WRITE_112 = 1.401        # Avro 1.1.2 writetable, 1M rows {id,x,name,flag}, null codec (2026-08-22, M-series host, Julia 1.12.6)
    BASE_READMAT_112 = 5.691      # Avro 1.1.2 readtable + columntable, same file
    cold = joinpath(@__DIR__, "perf", "cold.jl")
    project = Base.active_project()
    function coldmetric(metric; threads=1, runs=5)
        vals = Float64[]
        extra = String[]
        for _ in 1:runs
            out = read(`$(Base.julia_cmd()) --startup-file=no --threads=$threads --project=$project $cold $metric`, String)
            fields = split(strip(out))
            @assert fields[1] == metric
            push!(vals, parse(Float64, fields[2]))
            extra = String[String(f) for f in fields[3:end]]
        end
        return (sort(vals)[cld(length(vals), 2)], extra)
    end
    twrite, _ = coldmetric("write")
    @test twrite <= BASE_WRITE_112 / 4                              # ≥ 4× vs 1.1.2
    ttable, _ = coldmetric("table1")
    @test ttable <= BASE_READMAT_112 / 10                           # ≥ 10× vs 1.1.2 read + materialise
    ratio8 = nothing
    if Sys.CPU_THREADS >= 8
        ratio8, _ = coldmetric("table8"; threads=8)
        @test ratio8 >= 3                                           # 8 tasks vs 1, 4 GiB limits on both
    end
    overheads = Dict{String,Float64}()
    for codec in ("zstandard", "deflate", "snappy")
        o, _ = coldmetric("codec-$codec"; runs=3)
        overheads[codec] = o
        @test o <= 1.3                                              # ≤ 1.3 × (null table + isolated transcode)
    end
    projratio, _ = coldmetric("projection")
    @test projratio >= 2                                            # select=(:id,) under :fast
    kern, kx = coldmetric("kernels"; runs=3)
    decallocs = kern
    dns, ea, ens, onedec, oneenc, parseus = parse.(Float64, kx)
    @test decallocs <= 1                                            # prepared typed decode: the string only
    @test ea == 0                                                   # prepared typed encode: zero allocations
    tload, _ = coldmetric("load")
    @test tload <= 0.5                                              # informational budget, asserted as sanity
    tttft, _ = coldmetric("ttft"; runs=3)
    @test tttft <= 1.5
    @info "§10.1 medians (5 cold processes)" write_s = round(twrite; digits=3) write_ratio_vs_112 = round(BASE_WRITE_112 / twrite; digits=1) table1_s = round(ttable; digits=3) table_ratio_vs_112 = round(BASE_READMAT_112 / ttable; digits=1) ratio_8t = ratio8 === nothing ? "n/a" : round(ratio8; digits=2) codec_overheads = join(("$k=$(round(v; digits=2))" for (k, v) in overheads), " ") projection_ratio = round(projratio; digits=1) prepared_decode = "$(decallocs) allocs, $(round(dns; digits=0)) ns" prepared_encode = "$(ea) allocs, $(round(ens; digits=0)) ns" oneshot = "dec $(round(onedec; digits=0)) ns, enc $(round(oneenc; digits=0)) ns" parseschema_us = round(parseus; digits=1) load_s = round(tload; digits=2) ttft_s = round(tttft; digits=2)
end
