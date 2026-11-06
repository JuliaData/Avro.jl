#!/usr/bin/env julia
# Cross-language Avro benchmark driver: Avro.jl against fastavro, Apache Avro Java, apache-avro (Rust),
# arrow-avro (Rust) and hamba/avro (Go). Generates the data once, runs every side in alternating rounds, and writes
# results/<run>.tsv plus the tables in README.md.
#
# usage: julia run.jl [--quick] [--avro PATH] [--runs N] [--shapes a,b] [--codecs a,b] [--sides a,b]
#                     [--ops read,write] [--maxload L]
#        julia run.jl --report results/<run>.tsv
# `--avro` defaults to the Avro.jl checkout this directory belongs to.
using Dates, Printf, Statistics

const ROOT = @__DIR__
const SHAPES = ["numeric", "strings", "mixed", "nested"]
const CODECS = ["null", "deflate", "zstandard", "snappy"]
const WRITE_CODECS = ["null", "deflate"]
# Sized so the fastest single-threaded reader or writer takes roughly 0.3 s.
const READ_ROWS = Dict("numeric" => 10_000_000, "strings" => 2_000_000, "mixed" => 4_000_000, "nested" => 3_000_000)
const WRITE_ROWS = Dict("numeric" => 4_000_000, "strings" => 1_000_000, "mixed" => 2_000_000, "nested" => 2_000_000)

datafile(shape, codec) = joinpath(ROOT, "data", "$shape-$codec.avro")
writeinput(shape) = joinpath(ROOT, "data", "write-$shape.avro")
outfile(side, shape, codec) = joinpath(ROOT, "data", "out", "$side-$shape-$codec.avro")

python() = `$(joinpath(ROOT, ".venv", "bin", "python")) -I $(joinpath(ROOT, "python", "bench.py"))`
juliabench(threads) = `julia --project=$(joinpath(ROOT, "julia")) --startup-file=no -t $threads $(joinpath(ROOT, "julia", "bench.jl"))`
rustbench() = `$(joinpath(ROOT, "rust", "target", "release", "peerbench"))`
gobench() = `$(joinpath(ROOT, "go", "peerbench"))`
function javabench(props...)
    java = haskey(ENV, "JAVA_HOME") ? joinpath(ENV["JAVA_HOME"], "bin", "java") : "java"
    cp = joinpath(ROOT, "java", "target", "classes") * ":" * readchomp(joinpath(ROOT, "java", "cp.txt"))
    return `$java --enable-native-access=ALL-UNNAMED -Dorg.apache.avro.SERIALIZABLE_PACKAGES=peerbench $(collect(props)) -cp $cp Bench`
end
const CC = ("-Dorg.apache.avro.fastread=false", "-Dorg.apache.avro.specific.use_custom_coders=true")

# Each side: a label for the tables, iterations per process (iteration 1 is the first call; the warm time
# is the median of the last three), and the command without its trailing iteration count.
const READ_SIDES = [
    (name="avrojl-table", label="Avro.jl Table, 1 thread", kind="columnar", iters=4,
     cmd=(s, f) -> `$(juliabench(1)) read table $s $f`),
    (name="avrojl-table-mt", label="Avro.jl Table, -t auto (MULTI-THREADED)", kind="columnar MT", iters=4,
     cmd=(s, f) -> `$(juliabench("auto")) read table $s $f`),
    (name="avrojl-table-typed", label="Avro.jl Table(path, T) typed columns, 1 thread", kind="columnar", iters=4,
     cmd=(s, f) -> `$(juliabench(1)) read table-typed $s $f`),
    (name="avrojl-table-typed-mt", label="Avro.jl Table(path, T) typed columns, -t auto (MULTI-THREADED)", kind="columnar MT", iters=4,
     cmd=(s, f) -> `$(juliabench("auto")) read table-typed $s $f`),
    (name="avrojl-rows-typed", label="Avro.jl Rows(path, T) typed", kind="rows typed", iters=4,
     cmd=(s, f) -> `$(juliabench(1)) read rows-typed $s $f`),
    (name="avrojl-rows-generic", label="Avro.jl Rows(path) Record", kind="rows generic", iters=4,
     cmd=(s, f) -> `$(juliabench(1)) read rows-generic $s $f`),
    (name="fastavro", label="fastavro reader (dicts)", kind="rows generic", iters=2,
     cmd=(s, f) -> `$(python()) read $s $f`),
    (name="java-generic", label="Java GenericRecord, reuse", kind="rows generic", iters=8,
     cmd=(s, f) -> `$(javabench()) read generic $s $f`),
    (name="java-specific", label="Java SpecificRecord, reuse (fast reader)", kind="rows typed", iters=8,
     cmd=(s, f) -> `$(javabench()) read specific $s $f`),
    (name="java-specific-cc", label="Java SpecificRecord, reuse (custom coders)", kind="rows typed", iters=8,
     cmd=(s, f) -> `$(javabench(CC...)) read specific $s $f`),
    (name="rs-apache-avro", label="Rust apache-avro Value", kind="rows generic", iters=3,
     cmd=(s, f) -> `$(rustbench()) read value $f`),
    (name="rs-arrow-avro", label="Rust arrow-avro RecordBatch", kind="columnar", iters=3,
     cmd=(s, f) -> `$(rustbench()) read arrow $f`),
    (name="go-hamba", label="Go hamba/avro typed struct (optional)", kind="rows typed", iters=3,
     cmd=(s, f) -> `$(gobench()) read $s $f`),
]

const WRITE_SIDES = [
    (name="avrojl-write-table", label="Avro.jl write(NamedTuple of Vectors), 1 thread", kind="columns", iters=3,
     cmd=(s, i, o, c) -> `$(juliabench(1)) write table $s $i $o $c`),
    (name="avrojl-write-table-mt", label="Avro.jl write(NamedTuple of Vectors), -t auto (MULTI-THREADED)", kind="columns MT", iters=3,
     cmd=(s, i, o, c) -> `$(juliabench("auto")) write table $s $i $o $c`),
    (name="avrojl-writer", label="Avro.jl Writer push!(struct), 1 thread", kind="rows typed", iters=3,
     cmd=(s, i, o, c) -> `$(juliabench(1)) write writer $s $i $o $c`),
    (name="fastavro", label="fastavro writer (dicts)", kind="rows generic", iters=2,
     cmd=(s, i, o, c) -> `$(python()) write $s $i $o $c`),
    (name="java-generic", label="Java GenericRecord DataFileWriter", kind="rows generic", iters=5,
     cmd=(s, i, o, c) -> `$(javabench()) write generic $s $i $o $c`),
    (name="java-specific", label="Java SpecificRecord DataFileWriter", kind="rows typed", iters=5,
     cmd=(s, i, o, c) -> `$(javabench()) write specific $s $i $o $c`),
    (name="java-specific-cc", label="Java SpecificRecord DataFileWriter (custom coders)", kind="rows typed", iters=5,
     cmd=(s, i, o, c) -> `$(javabench(CC...)) write specific $s $i $o $c`),
    (name="rs-apache-avro", label="Rust apache-avro Writer(Value)", kind="rows generic", iters=3,
     cmd=(s, i, o, c) -> `$(rustbench()) write value $s $i $o $c`),
    (name="rs-arrow-avro", label="Rust arrow-avro AvroWriter(RecordBatch)", kind="columns", iters=3,
     cmd=(s, i, o, c) -> `$(rustbench()) write arrow $s $i $o $c`),
    (name="go-hamba", label="Go hamba/avro Encoder(struct) (optional)", kind="rows typed", iters=3,
     cmd=(s, i, o, c) -> `$(gobench()) write $s $i $o $c`),
]

loadavg() = parse.(Float64, split(strip(readchomp(`sysctl -n vm.loadavg`), ['{', '}', ' ']))[1:2])

function waitforload(maxload)
    for _ in 1:40
        (l = loadavg()[1]) <= maxload && return
        @info "load $l > $maxload; waiting 15 s"
        sleep(15)
    end
end

# Run `cmd` under /usr/bin/time -l; return its iteration lines, Julia load time, and process counters.
function measure(cmd)
    out, err = IOBuffer(), IOBuffer()
    ok = success(pipeline(ignorestatus(`/usr/bin/time -l $cmd`); stdout=out, stderr=err))
    so, se = String(take!(out)), String(take!(err))
    iters = Vector{Dict{String,Any}}()
    load = NaN
    for line in eachline(IOBuffer(so))
        f = split(line)
        isempty(f) && continue
        if f[1] == "load"
            load = parse(Float64, f[2])
        elseif f[1] == "iter" && length(f) >= 7
            d = Dict{String,Any}("s" => parse(Float64, f[3]), "rows" => parse(Int, f[4]), "isum" => f[5],
                                 "nstr" => f[6], "nbytes" => f[7])
            for kv in f[8:end]
                k, v = split(kv, '='; limit=2)
                d[k] = parse(Float64, v)
            end
            push!(iters, d)
        end
    end
    m(re) = (x = match(re, se); x === nothing ? NaN : parse(Float64, x[1]))
    proc = (wall=m(r"([\d.]+) real"), user=m(r"([\d.]+) user"), sys=m(r"([\d.]+) sys"),
            rss=m(r"(\d+)\s+maximum resident set size"))
    ok = ok && !isempty(iters) && all(d -> (d["isum"], d["nstr"], d["nbytes"], d["rows"]) ==
                                           (iters[1]["isum"], iters[1]["nstr"], iters[1]["nbytes"], iters[1]["rows"]), iters)
    note = ok ? "" : replace(last(strip(se), 300), r"\s+" => " ")
    return (; ok, iters, load, proc, note)
end

const COLUMNS = ["run", "utc", "op", "shape", "codec", "side", "round", "ok", "rows", "isum", "nstr", "nbytes",
                 "first_s", "warm_s", "warm_lo", "warm_hi", "n_warm", "jl_load_s", "file_bytes", "out_bytes",
                 "proc_wall_s", "user_s", "sys_s", "maxrss_bytes", "gc_frac", "alloc_bytes", "load1_before",
                 "load1_after", "avro_rev", "note"]

function record!(io, row)
    println(io, join((replace(string(get(row, c, "")), '\t' => ' ') for c in COLUMNS), '\t'))
    flush(io)
end

function runside(io, base, op, shape, codec, side, round, cmd, filebytes, outpath, maxload)
    waitforload(maxload)
    l0 = loadavg()[1]
    r = measure(`$cmd $(side.iters)`)
    l1 = loadavg()[1]
    row = merge(base, Dict("op" => op, "shape" => shape, "codec" => codec, "side" => side.name, "round" => round,
                           "utc" => Dates.format(now(UTC), "yyyy-mm-ddTHH:MM:SS"), "ok" => r.ok, "note" => r.note,
                           "file_bytes" => filebytes, "jl_load_s" => r.load, "proc_wall_s" => r.proc.wall,
                           "user_s" => r.proc.user, "sys_s" => r.proc.sys, "maxrss_bytes" => r.proc.rss,
                           "load1_before" => l0, "load1_after" => l1))
    if !isempty(r.iters)
        warm = r.iters[max(2, end - 2):end]
        length(r.iters) == 1 && (warm = r.iters)
        ws = [d["s"] for d in warm]
        merge!(row, Dict("rows" => r.iters[1]["rows"], "isum" => r.iters[1]["isum"], "nstr" => r.iters[1]["nstr"],
                         "nbytes" => r.iters[1]["nbytes"], "first_s" => r.iters[1]["s"], "warm_s" => median(ws),
                         "warm_lo" => minimum(ws), "warm_hi" => maximum(ws), "n_warm" => length(ws),
                         "gc_frac" => haskey(warm[end], "gc") ? median([d["gc"] / d["s"] for d in warm]) : "",
                         "alloc_bytes" => haskey(warm[end], "alloc") ? median([d["alloc"] for d in warm]) : "",
                         "out_bytes" => outpath === nothing ? "" : filesize(outpath)))
    end
    record!(io, row)
    @printf("%-6s %-8s %-9s %-22s r%d %s warm=%s first=%s rss=%.0fMB load=%.1f\n", op, shape, codec, side.name, round,
            r.ok ? "ok" : "FAIL", get(row, "warm_s", "-"), get(row, "first_s", "-"), r.proc.rss / 2^20, l0)
    r.ok || println("   ", r.note)
    return row
end

# Every side of a round starts one later than the previous round's, so no side always follows the same one.
rotated(v, r) = [v[mod1(i + r - 1, length(v))] for i in eachindex(v)]

function checksum_of(path, shape; reader=:rust)
    cmd = reader === :rust ? `$(rustbench()) read value $path 1` : `$(python()) read $shape $path 1`
    r = measure(cmd)
    r.ok || return nothing
    d = r.iters[1]
    return (d["rows"], d["isum"], d["nstr"], d["nbytes"])
end

function setup(avro)
    if !isfile(joinpath(ROOT, ".venv", "bin", "python"))
        run(`python3 -m venv $(joinpath(ROOT, ".venv"))`)
        run(`$(joinpath(ROOT, ".venv", "bin", "pip")) install -q fastavro==1.13.1 cramjam==2.14.0`)
    end
    run(Cmd(`cargo build --release -q`; dir=joinpath(ROOT, "rust")))
    run(Cmd(`go build -o peerbench .`; dir=joinpath(ROOT, "go")))
    javadir = joinpath(ROOT, "java")
    run(Cmd(`mvn -q -B package -DskipTests`; dir=javadir))
    run(Cmd(`mvn -q -B dependency:build-classpath -Dmdep.outputFile=cp.txt`; dir=javadir))
    run(`julia --project=$(joinpath(ROOT, "julia")) --startup-file=no -t 1 -e "using Pkg; Pkg.develop(path=ARGS[1]); Pkg.instantiate(); Pkg.precompile()" $avro`)
    rev = readchomp(`git -C $avro rev-parse --short HEAD`)
    dirty = !isempty(readchomp(`git -C $avro status --porcelain --untracked-files=no`))
    return rev * (dirty ? "+dirty" : "")
end

function gendata(shapes)
    mkpath(joinpath(ROOT, "data", "out"))
    jobs = [(s, c, READ_ROWS[s], datafile(s, c)) for s in shapes for c in CODECS]
    append!(jobs, [(s, "null", WRITE_ROWS[s], writeinput(s)) for s in shapes])
    todo = filter(j -> !isfile(j[4]), jobs)
    isempty(todo) && return
    @info "generating $(length(todo)) data files"
    py = joinpath(ROOT, ".venv", "bin", "python")
    asyncmap(todo; ntasks=8) do (s, c, n, path)
        run(`$py -I $(joinpath(ROOT, "gen.py")) $(joinpath(ROOT, "schemas")) $s $n $c $path`)
    end
end

function main(args)
    i = findfirst(==("--report"), args)
    i === nothing || return report(args[i + 1])
    opt(name, default) = (j = findfirst(==(name), args); j === nothing ? default : args[j + 1])
    quick = "--quick" in args
    avro = abspath(opt("--avro", joinpath(ROOT, "..", "..")))
    runs = parse(Int, opt("--runs", quick ? "2" : "5"))
    shapes = split(opt("--shapes", quick ? "mixed,strings" : join(SHAPES, ",")), ',')
    codecs = split(opt("--codecs", quick ? "null,deflate" : join(CODECS, ",")), ',')
    ops = split(opt("--ops", "read,write"), ',')
    sidefilter = opt("--sides", "")
    keep(sides) = isempty(sidefilter) ? sides : filter(s -> s.name in split(sidefilter, ','), sides)
    maxload = parse(Float64, opt("--maxload", "20"))

    rev = setup(avro)
    gendata(shapes)
    run_id = Dates.format(now(), "yyyymmdd-HHMMSS") * (quick ? "-quick" : "")
    tsv = joinpath(ROOT, "results", "$run_id.tsv")
    mkpath(dirname(tsv))
    base = Dict{String,Any}("run" => run_id, "avro_rev" => rev)
    open(tsv, "w") do io
        println(io, join(COLUMNS, '\t'))
        if "read" in ops
            for shape in shapes, codec in codecs, round in 1:runs, side in rotated(keep(READ_SIDES), round)
                f = datafile(shape, codec)
                runside(io, base, "read", shape, codec, side, round, side.cmd(shape, f), filesize(f), nothing, maxload)
            end
        end
        if "write" in ops
            for shape in shapes
                expected = checksum_of(writeinput(shape), shape)
                for codec in filter(in(WRITE_CODECS), codecs), round in 1:runs, side in rotated(keep(WRITE_SIDES), round)
                    o = outfile(side.name, shape, codec)
                    row = runside(io, base, "write", shape, codec, side, round, side.cmd(shape, writeinput(shape), o, codec),
                                  filesize(writeinput(shape)), o, maxload)
                    round == 1 || continue
                    # a different implementation reads the written file back
                    got = isfile(o) ? checksum_of(o, shape; reader=side.name == "rs-apache-avro" ? :fastavro : :rust) : nothing
                    record!(io, merge(base, Dict("op" => "xcheck", "shape" => shape, "codec" => codec, "side" => side.name,
                                                 "round" => 1, "ok" => got !== nothing && got == expected,
                                                 "note" => "expected $expected got $got", "out_bytes" => isfile(o) ? filesize(o) : "")))
                    got == expected || println("   CROSS-CHECK FAILED $(side.name) $shape $codec: expected $expected got $got")
                end
            end
        end
    end
    report(tsv)
end

# ---- tables -------------------------------------------------------------------------------------------

function readtsv(path)
    lines = readlines(path)
    hdr = split(lines[1], '\t')
    return [Dict(zip(hdr, split(l, '\t'; keepempty=true))) for l in lines[2:end]]
end

num(x) = (v = tryparse(Float64, x); v === nothing ? NaN : v)
fmt_s(x) = isnan(x) ? "-" : x >= 10 ? @sprintf("%.1f", x) : x >= 1 ? @sprintf("%.2f", x) : @sprintf("%.3f", x)
fmt_rate(x) = isnan(x) ? "-" : x >= 1e6 ? @sprintf("%.1fM", x / 1e6) : @sprintf("%.0fk", x / 1e3)

# Avro.jl's best time against the best peer's, overall and within each kind of API.
function summary!(io, runs)
    kinds = Dict((s.name, op) => s.kind for (op, sides) in (("read", READ_SIDES), ("write", WRITE_SIDES)) for s in sides)
    acc = Dict{NTuple{4,String},Vector{Float64}}()
    outs = Dict{NTuple{4,String},Vector{Float64}}()
    for r in runs
        k = (r["op"], r["shape"], r["codec"], r["side"])
        push!(get!(acc, k, Float64[]), num(r["warm_s"]))
        push!(get!(outs, k, Float64[]), num(r["out_bytes"]))
    end
    med = Dict(k => median(v) for (k, v) in acc)
    out = Dict(k => median(v) for (k, v) in outs)
    println(io, "### Summary\n")
    println(io, "Each cell is Avro.jl's median warm seconds / the fastest peer's, then how many times faster Avro.jl is ",
            "(×; below 1 means slower). Every column is single-threaded except the last. A compressed write counts as a ",
            "peer only if its file is within 3% of the cell's median file size, so a writer that compresses less ",
            "cannot win on speed alone.\n")
    println(io, "| op | shape | codec | fastest Avro.jl / fastest peer | columnar | typed rows | generic rows | Avro.jl MULTI-THREADED |")
    println(io, "|---|---|---|---|---|---|---|---|")
    for op in ("read", "write"), shape in SHAPES, codec in CODECS
        sides = [k[4] for k in keys(med) if k[1:3] == (op, shape, codec)]
        isempty(sides) && continue
        if op == "write" && codec != "null"
            typical = median([out[(op, shape, codec, s)] for s in sides])
            filter!(s -> out[(op, shape, codec, s)] <= 1.03 * typical, sides)
        end
        kind(s) = kinds[(s, op)]
        avro(s) = startswith(s, "avrojl")
        mt(s) = endswith(kind(s), "MT")
        function best(pred)
            c = filter(pred, sides)
            isempty(c) && return nothing
            s = argmin(s -> med[(op, shape, codec, s)], c)
            return (s, med[(op, shape, codec, s)])
        end
        cell(a, p) = a === nothing || p === nothing ? "-" :
            @sprintf("%s / %s %s (×%.2f)", fmt_s(a[2]), fmt_s(p[2]), p[1], p[2] / a[2])
        col = op == "read" ? "columnar" : "columns"
        cells = [cell(best(s -> avro(s) && !mt(s)), best(s -> !avro(s)))]
        for k in (col, "rows typed", "rows generic")
            push!(cells, cell(best(s -> avro(s) && kind(s) == k), best(s -> !avro(s) && kind(s) == k)))
        end
        m = best(s -> avro(s) && mt(s))
        push!(cells, m === nothing ? "-" : fmt_s(m[2]))
        println(io, "| ", op, " | ", shape, " | ", codec, " | ", join(cells, " | "), " |")
    end
    println(io)
end

function report(tsv)
    rows = readtsv(tsv)
    runs = filter(r -> r["op"] in ("read", "write") && r["ok"] == "true", rows)
    fails = filter(r -> r["op"] in ("read", "write") && r["ok"] != "true", rows)
    labels = Dict((s.name, op) => s.label for (op, sides) in (("read", READ_SIDES), ("write", WRITE_SIDES)) for s in sides)
    io = IOBuffer()
    println(io, "Run `", rows[1]["run"], "`, Avro.jl `", rows[1]["avro_rev"], "`. Each time is the median over runs of a ",
            "process's warm time in seconds (the median of its last three iterations), with the range of those per-run values. ",
            "\"First\" is the process's first iteration, compilation and JIT warm-up included. Rows/s and MB/s use the warm ",
            "median and the bytes of the input file (reads) or of the written file (writes). Peak RSS is the whole process. ",
            "Load is the 1-minute load average before the run.\n")
    summary!(io, runs)
    for op in ("read", "write"), shape in SHAPES
        sel = filter(r -> r["op"] == op && r["shape"] == shape, runs)
        isempty(sel) && continue
        println(io, "### ", op, " ", shape, "\n")
        println(io, "| codec | implementation | warm s (median) | range | first s | rows/s | MB/s | file MB | peak RSS MB | GC % | load | runs |")
        println(io, "|---|---|---|---|---|---|---|---|---|---|---|---|")
        for codec in CODECS
            cs = filter(r -> r["codec"] == codec, sel)
            isempty(cs) && continue
            groups = Dict{String,Vector{Dict}}()
            for r in cs
                push!(get!(groups, r["side"], Dict[]), r)
            end
            meds = Dict(k => median(num.(getindex.(v, "warm_s"))) for (k, v) in groups)
            for side in sort(collect(keys(groups)); by=k -> meds[k])
                g = groups[side]
                w = num.(getindex.(g, "warm_s"))
                m = median(w)
                nrows = num(g[1]["rows"])
                bytes = op == "read" ? num(g[1]["file_bytes"]) : median(num.(getindex.(g, "out_bytes")))
                gc = filter(!isnan, num.(getindex.(g, "gc_frac")))
                @printf(io, "| %s | %s | %s | %s to %s | %s | %s | %.0f | %.1f | %.0f | %s | %.1f | %d |\n", codec,
                        get(labels, (side, op), side), fmt_s(m), fmt_s(minimum(w)), fmt_s(maximum(w)),
                        fmt_s(median(num.(getindex.(g, "first_s")))), fmt_rate(nrows / m), bytes / m / 1e6, bytes / 1e6,
                        median(num.(getindex.(g, "maxrss_bytes"))) / 2^20, isempty(gc) ? "-" : @sprintf("%.0f", 100 * median(gc)),
                        median(num.(getindex.(g, "load1_before"))), length(g))
            end
        end
        println(io)
    end
    # every reader of a file must agree on its checksum
    println(io, "### Checksums\n")
    for shape in SHAPES, codec in CODECS
        sel = filter(r -> r["op"] == "read" && r["shape"] == shape && r["codec"] == codec && r["ok"] == "true", rows)
        isempty(sel) && continue
        sums = unique([(r["rows"], r["isum"], r["nstr"], r["nbytes"]) for r in sel])
        println(io, "- read ", shape, " ", codec, ": ", length(sums) == 1 ? "all $(length(sel)) runs agree on $(only(sums))" :
                                                         "MISMATCH $(sums)")
    end
    for r in filter(r -> r["op"] == "xcheck", rows)
        println(io, "- write ", r["shape"], " ", r["codec"], " ", r["side"], ": ", r["ok"] == "true" ? "read back identical" : "MISMATCH, " * r["note"])
    end
    for r in fails
        println(io, "- FAILED ", r["op"], " ", r["shape"], " ", r["codec"], " ", r["side"], " round ", r["round"], ": ", r["note"])
    end
    tables = String(take!(io))
    write(replace(tsv, r"\.tsv$" => "-tables.md"), tables)
    results = joinpath(ROOT, "README.md")
    if isfile(results)
        text = read(results, String)
        a, b = findfirst("<!-- tables:begin -->", text), findfirst("<!-- tables:end -->", text)
        if a !== nothing && b !== nothing
            write(results, text[1:last(a)] * "\n" * tables * "\n" * text[first(b):end])
        end
    end
    println(tables)
end

main(ARGS)
