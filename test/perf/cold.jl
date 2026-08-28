# One cold-process §10.1 measurement: `julia --startup-file=no cold.jl <metric>` prints `metric value`.
# Workload metrics report the best of 3 in-process repetitions after one warm-up; load and time-to-
# first-table measure cold startup directly. The driver takes the median of 5 cold processes. Kernels
# use prepared objects built outside the timed region.

metric = ARGS[1]

if metric == "load"
    t = @elapsed @eval using Avro
    println("load ", t)
    exit(0)
elseif metric == "ttft"
    t0 = time()
    @eval begin
        using Avro, Tables
        Tables.columntable(Avro.Table(Avro.tobuffer([(a=Int64(1), b="x")])))
    end
    println("ttft ", time() - t0)
    exit(0)
end

using Avro, Tables

const S4 = Avro.parseschema("""{"type":"record","name":"Bench","fields":[
    {"name":"id","type":"long"},{"name":"x","type":"double"},
    {"name":"name","type":"string"},{"name":"flag","type":"boolean"}]}""")
const N = 1_000_000
function rows()
    return [(id=Int64(i), x=i / 7, name="name-$(i % 1000)", flag=isodd(i)) for i in 1:N]
end

function raisedlimits()
    return Avro.Limits(max_total_bytes=4 << 30, max_block_bytes=16 << 20, max_block_output_bytes=256 << 20,
                       max_codec_memory=32 << 20, max_bytes=64 << 20, max_datum_bytes=64 << 20)
end

function bestof3(f)
    f()
    best = Inf
    for _ in 1:3
        GC.gc()
        t0 = time_ns()
        f()
        best = min(best, (time_ns() - t0) / 1e9)
    end
    return best
end

function bestallocsof3(f, args...)
    f(args...)
    best = typemax(Int)
    for _ in 1:3
        GC.gc()
        best = min(best, @allocations f(args...))
    end
    return best
end

if metric == "write"
    data = rows()
    file = joinpath(mktempdir(), "w.avro")
    println("write ", bestof3(() -> Avro.write(file, data; schema=S4)))
elseif metric == "table1"
    file = joinpath(mktempdir(), "t.avro")
    Avro.write(file, rows(); schema=S4)
    println("table1 ", bestof3(() -> Avro.Table(file; ntasks=1)))
elseif metric == "table8"
    lim = raisedlimits()
    file = joinpath(mktempdir(), "t8.avro")
    Avro.write(file, rows(); schema=S4, block_bytes=1 << 20, limits=lim)
    t1 = bestof3(() -> Avro.Table(file; ntasks=1, limits=lim))
    t8 = bestof3(() -> Avro.Table(file; ntasks=8, limits=lim))
    println("table8 ", t1 / t8)
elseif startswith(metric, "codec-")
    codec = Symbol(split(metric, "-")[2])
    dir = mktempdir()
    nullf, codecf = joinpath(dir, "n.avro"), joinpath(dir, "c.avro")
    data = rows()
    Avro.write(nullf, data; schema=S4)
    Avro.write(codecf, data; schema=S4, codec=codec)
    tnull = bestof3(() -> Avro.Table(nullf; ntasks=1))
    tcodec = bestof3(() -> Avro.Table(codecf; ntasks=1))
    r = Avro.Reader(codecf)
    entries = Avro.prescanblocks(r).entries
    src = r.source
    function transcode1()
        for e in entries
            Avro.decompressblock(r.codecname, r.codec, view(src.buf, e.offset:e.offset + e.size - 1), r.limits, r.budget)
            Avro.release!(r.budget, r.budget.reserved)
        end
        return nothing
    end
    ttrans = bestof3(transcode1)
    close(r)
    println(metric, " ", tcodec / (tnull + ttrans))
elseif metric == "projection"
    file = joinpath(mktempdir(), "p.avro")
    Avro.write(file, rows(); schema=S4)
    tsel = bestof3(() -> Avro.Table(file; select=(:id,), validate=:fast, ntasks=1))
    tfull = bestof3(() -> Avro.Table(file; validate=:fast, ntasks=1))
    println("projection ", tfull / tsel)
elseif metric == "kernels"
    T = @NamedTuple{id::Int64, x::Float64, name::String, flag::Bool}
    dr = Avro.DatumReader(S4, T)
    dw = Avro.DatumWriter(S4, T)
    v = (id=Int64(1), x=2.0, name="abcd", flag=true)
    bytes = Avro.encode(S4, v)
    enc = Avro.Encoder(Avro.Budget(Avro.Limits(); direction=:encode))
    function deck(dr, bytes, n)
        acc = 0
        for _ in 1:n
            u = dr(bytes)
            acc += u.id + sizeof(u.name)
        end
        return acc
    end

    function enck(dw, enc, v, n)
        for i in 1:n
            dw(enc, v)
            i % 4096 == 0 && take!(enc)
        end
        return enc.pos
    end
    deck(dr, bytes, 10)
    enck(dw, enc, v, 5000)                             # steady state: the reused encoder is fully grown
    take!(enc)
    da = bestallocsof3(deck, dr, bytes, 1000) / 1000
    dns = bestof3(() -> deck(dr, bytes, 200_000)) * 1e9 / 200_000
    ea = bestallocsof3(enck, dw, enc, v, 1000) / 1000
    take!(enc)
    ens = bestof3(() -> enck(dw, enc, v, 200_000)) * 1e9 / 200_000
    # one-shot (informational): plan construction per call
    function oneshotdecode(n)
        for _ in 1:n
            Avro.decode(S4, bytes)
        end
        return nothing
    end

    function oneshotencode(n)
        for _ in 1:n
            Avro.encode(S4, v)
        end
        return nothing
    end
    oneshot_dec = bestof3(() -> oneshotdecode(200)) * 1e9 / 200
    oneshot_enc = bestof3(() -> oneshotencode(200)) * 1e9 / 200
    avsc = read(joinpath(@__DIR__, "..", "fixtures", "apache", "interop.avsc"), String)
    function parsebatch(n)
        for _ in 1:n
            Avro.parseschema(avsc)
        end
        return nothing
    end
    tparse = bestof3(() -> parsebatch(50)) * 1e6 / 50
    parseallocs = bestallocsof3(parsebatch, 50) / 50
    println("kernels ", da, " ", dns, " ", ea, " ", ens, " ", oneshot_dec, " ", oneshot_enc, " ", tparse, " ", parseallocs)
end
