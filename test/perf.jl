# The §10.2 performance gates (opt-in: AVRO_PERF=true on the named authoring host). Ratios compare
# against the Avro 1.1.2 baselines measured on the same host and Julia (recorded below); timings are
# best-of-3 after a warm-up. Kernel allocation gates run unconditionally in typed/columns tests; here
# the prepared-object forms are gated and the informational absolutes are logged.

@testset "Performance gates (plan §10.2)" begin
    BASE_WRITE_112 = 1.401        # Avro 1.1.2 writetable, 1M rows, null codec (2026-08-22, M-series host, Julia 1.12.6)
    BASE_READMAT_112 = 5.691      # Avro 1.1.2 readtable + columntable, same file
    n = 1_000_000
    rows = [(id=Int64(i), name="name-$(i % 1000)", score=i / 7, flag=isodd(i)) for i in 1:n]
    s = Avro.parseschema("{\"type\":\"record\",\"name\":\"Bench\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"score\",\"type\":\"double\"},{\"name\":\"flag\",\"type\":\"boolean\"}]}")
    raised = Avro.Limits(max_total_bytes=4 << 30, max_block_bytes=16 << 20, max_block_output_bytes=256 << 20,
                         max_codec_memory=32 << 20, max_bytes=64 << 20, max_datum_bytes=64 << 20)
    dir = mktempdir()
    file = joinpath(dir, "perf.avro")
    bestof3(f) = minimum((f(); GC.gc(); t0 = time(); f(); time() - t0) for _ in 1:3)
    twrite = bestof3(() -> Avro.write(file, rows; schema=s))
    @test twrite <= BASE_WRITE_112 / 4                              # ≥ 4× vs 1.1.2
    ttable = bestof3(() -> Avro.Table(file; ntasks=1))
    @test ttable <= BASE_READMAT_112 / 10                           # ≥ 10× vs 1.1.2 read+materialise
    ratio8 = nothing
    if Threads.nthreads() >= 8
        big = joinpath(dir, "perf8.avro")
        Avro.write(big, rows; schema=s, block_bytes=4 << 20, limits=raised)
        t1 = bestof3(() -> Avro.Table(big; ntasks=1, limits=raised))
        t8 = bestof3(() -> Avro.Table(big; ntasks=8, limits=raised))
        ratio8 = t1 / t8
        @test ratio8 >= 3                                           # 8 tasks vs 1, 4 GiB limits on both
    end
    tselfast = bestof3(() -> Avro.Table(file; select=(:id,), validate=:fast))
    tfullfast = bestof3(() -> Avro.Table(file; validate=:fast))
    tselstrict = bestof3(() -> Avro.Table(file; select=(:id,), validate=:strict))
    @test tfullfast / tselfast >= 2                                 # projection ≥ 2× under :fast
    # codec read overhead: a zstandard table ≤ 1.3 × (null table + the isolated transcode of its blocks)
    zfile = joinpath(dir, "perfz.avro")
    Avro.write(zfile, rows; schema=s, codec=:zstandard)
    tz = bestof3(() -> Avro.Table(zfile))
    r = Avro.Reader(zfile)
    entries = Avro.prescanblocks(r).entries
    src = r.source
    transcode1() = for e in entries
        Avro.decompressblock(r.codecname, r.codec, view(src.buf, e.offset:e.offset + e.size - 1), r.limits, r.budget)
        Avro.release!(r.budget, r.budget.reserved)
    end
    ttrans = bestof3(transcode1)
    close(r)
    @test tz <= 1.3 * (ttable + ttrans)
    # prepared kernels
    T = @NamedTuple{id::Int64, name::String, score::Float64, flag::Bool}
    dw = Avro.DatumWriter(s, T)
    enc = Avro.Encoder(Avro.Budget(Avro.Limits(); direction=:encode))
    v = rows[1]
    dw(enc, v)
    take!(enc)
    dw(enc, v)
    @test (@allocated dw(enc, v)) == 0                              # prepared typed encode: zero allocations
    dr = Avro.DatumReader(s, T)
    one = Avro.encode(s, v)
    dr(one)
    decallocs = @allocations dr(one)
    avsc = read(joinpath(@__DIR__, "fixtures", "apache", "interop.avsc"), String)
    Avro.parseschema(avsc)
    tparse = minimum((t0 = time_ns(); Avro.parseschema(avsc); (time_ns() - t0) / 1000) for _ in 1:50)
    @info "§10.2 measurements" write_s = round(twrite; digits=3) table1_s = round(ttable; digits=3) ratio_vs_112_write = round(BASE_WRITE_112 / twrite; digits=1) ratio_vs_112_table = round(BASE_READMAT_112 / ttable; digits=1) ratio_8t = ratio8 === nothing ? "n/a (needs ≥8 threads)" : round(ratio8; digits=2) projection_fast_ratio = round(tfullfast / tselfast; digits=1) select_strict_s = round(tselstrict; digits=4) zstd_overhead = round(tz / (ttable + ttrans); digits=2) prepared_decode_allocations = decallocs parseschema_us = round(tparse; digits=1)
end
