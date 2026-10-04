# The Phase 4c peak-RSS gate (plan §4.9, fixed method): a fresh warmed child process faults the input
# buffer before the baseline, the parent samples `ps -o rss=` every 10 ms between an acknowledged READY
# and DONE, and `peak − baseline ≤ effective_ceiling + 128 MiB` with an in-flight high-water ≥ 2.
# Opt-in: AVRO_RSS_GATE=true (Unix; needs a multi-thread-capable host).

@testset "Parallel peak RSS (plan §4.9)" begin
    lim = Avro.Limits(max_total_bytes=4 << 30, max_block_bytes=64 << 20, max_block_output_bytes=128 << 20,
                      max_codec_memory=64 << 20, max_bytes=256 << 20, max_datum_bytes=256 << 20)
    wlim = Avro.Limits(max_total_bytes=16 << 30, max_block_bytes=64 << 20, max_block_output_bytes=8 << 30,
                       max_codec_memory=64 << 20, max_bytes=256 << 20, max_datum_bytes=256 << 20)
    dir = mktempdir()
    file = joinpath(dir, "rss.avro")
    s = Avro.parseschema("{\"type\":\"record\",\"name\":\"RSS\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"string\"}]}")
    w = Avro.Writer(file, s; codec=:deflate, block_bytes=16 << 20, limits=wlim)
    payload = "x"^65000
    nrows = 33_000                                              # ≈ 2 GiB decoded: a half-ceiling table of ≈ 16 MiB blocks
    for i in 1:nrows
        push!(w, (a=Int64(i), b=payload))
    end
    close(w)
    @info "rss gate fixture" file_mb = round(filesize(file) / 1 << 20; digits=1) decoded_gb = round(nrows * 65000 / 1 << 30; digits=2)
    child = """
    using Avro, Tables
    Avro.Table(IOBuffer(take!(Avro.tobuffer([(a=Int64(1), b="warm")]; schema=Avro.parseschema(\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"W\\\",\\\"fields\\\":[{\\\"name\\\":\\\"a\\\",\\\"type\\\":\\\"long\\\"},{\\\"name\\\":\\\"b\\\",\\\"type\\\":\\\"string\\\"}]}\")))))   # warm-up decode
    buf = read($(repr(file)))
    acc = sum(view(buf, 1:4096:length(buf)); init=UInt(0))       # fault the caller-owned buffer before the baseline
    livehw = Ref(0)
    Avro.PARALLEL_HOOK[] = (ev, i) -> (ev === :commit && (livehw[] = max(livehw[], Int(Base.gc_live_bytes()))); nothing)
    println("READY ", acc % 7)
    flush(stdout)
    readline(stdin) == "GO" || error("no ack")
    lim = Avro.Limits(max_total_bytes=4 << 30, max_block_bytes=64 << 20, max_block_output_bytes=128 << 20,
                      max_codec_memory=64 << 20, max_bytes=256 << 20, max_datum_bytes=256 << 20)
    t = Avro.Table(buf; ntasks=8, limits=lim)
    st = Avro.LAST_PARALLEL_STATS[]
    println("DONE rows=", length(t), " maxrss=", Sys.maxrss(), " live=", livehw[],
            " inflight=", st.inflight_highwater, " nworkers=", st.nworkers, " peakviol=", st.peak_violations)
    flush(stdout)
    """
    childpath = joinpath(dir, "child.jl")
    write(childpath, child)
    cmd = `$(Base.julia_cmd()) --startup-file=no --threads=8 --project=$(Base.active_project()) $childpath`
    proc = open(cmd, "r+")
    pid = getpid(proc)
    function samplerss(pid)
        try
            return tryparse(Int, strip(read(`ps -o rss= -p $pid`, String)))
        catch
            return nothing
        end
    end
    ready = readline(proc)
    @test startswith(ready, "READY")
    samples = Int[]
    push!(samples, something(samplerss(pid), 0))
    baseline = samples[1] * 1024
    println(proc, "GO")
    flush(proc)
    done = ""
    while true
        rss = samplerss(pid)
        rss === nothing || push!(samples, rss)
        while bytesavailable(proc) > 0
            line = readline(proc)
            startswith(line, "DONE") && (done = line)
        end
        done == "" || break
        if process_exited(proc)
            while !eof(proc)
                line = readline(proc)
                startswith(line, "DONE") && (done = line)
            end
            done == "" && error("child exited without DONE")
            break
        end
        sleep(0.01)
    end
    wait(proc)
    @test startswith(done, "DONE")
    fields = Dict(split(kv, "=")[1] => split(kv, "=")[2] for kv in split(done)[2:end] if occursin("=", kv))
    maxrss = parse(Int, fields["maxrss"])
    livehw = parse(Int, fields["live"])
    peak = max(maximum(samples) * 1024, maxrss, livehw)
    ceiling = Avro.effective_ceiling(lim)
    @info "rss gate" baseline_mb = round(baseline / 1 << 20; digits=1) peak_mb = round(peak / 1 << 20; digits=1) ceiling_mb = ceiling ÷ 1 << 20 inflight = fields["inflight"] nworkers = fields["nworkers"]
    @test parse(Int, fields["rows"]) == nrows
    @test parse(Int, fields["peakviol"]) == 0
    @test parse(Int, fields["inflight"]) >= 2                    # at least two blocks concurrently in flight
    @test peak - baseline <= ceiling + (128 << 20)
end
