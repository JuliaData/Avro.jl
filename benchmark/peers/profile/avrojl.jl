# Profile one Avro.jl read or write path (not a reported measurement): the flat profile's hottest frames.
# usage: julia --project=julia -t 1 profile/avrojl.jl read  <table|rows-typed|rows-generic> <shape> <file> [mmap]
#        julia --project=julia -t 1 profile/avrojl.jl write <table|writer> <shape> <infile> <codec>
using Profile
include(joinpath(@__DIR__, "..", "julia", "bench.jl"))

function profiled(f; iters=3)
    f()                                            # compile outside the profile
    st = @timed f()
    println("one call: ", round(st.time; digits=3), " s, GC ", round(100 * st.gctime / st.time; digits=1), " %, ",
            round(st.bytes / 2^20; digits=1), " MiB allocated")
    Profile.clear()
    Profile.init(n=10^7, delay=0.0005)
    @profile for _ in 1:iters
        f()
    end
    io = IOBuffer()
    Profile.print(IOContext(io, :displaysize => (10_000, 220)); format=:flat, sortedby=:count, C=true, mincount=40)
    rows = [(parse(Int, m[1]), parse(Int, m[2]), m[3]) for l in split(String(take!(io)), '\n')
            for m in (match(r"^\s*(\d+)\s+(\d+)\s+(.*)$", l),) if m !== nothing]
    total = maximum(first, rows)
    gc = maximum((r[1] for r in rows if occursin(r"\b_?(ijl|jl)_gc_collect\b", r[3])); init=0)
    println("samples ", total, "; inside GC collection: ", round(100 * gc / total; digits=1), " %")
    println("top frames by self samples (self %, inclusive %, frame):")
    for (incl, self, frame) in first(sort(rows; by=r -> -r[2]), 25)
        println(lpad(round(100 * self / total; digits=1), 6), lpad(round(100 * incl / total; digits=1), 7), "  ",
                replace(strip(frame), r"\s{2,}" => " "))
    end
end

let a = ARGS
    if a[1] == "read"
        mode, shape, path = a[2], a[3], a[4]
        T = rowtype(shape)
        f = mode == "table" ? (() -> read_table(path, 1)) : mode == "rows-typed" ? (() -> read_typed(path, T)) :
            (() -> read_generic(path))
        profiled(f)
    else
        mode, shape, infile, codec = a[2], a[3], a[4], a[5]
        schema = Avro.parseschema(read(joinpath(@__DIR__, "..", "schemas", shape * ".avsc"), String))
        v = loadrows(infile, rowtype(shape))
        cols = columnsof(v)
        out = tempname() * ".avro"
        f = mode == "table" ? (() -> write_table(out, cols, schema, Symbol(codec))) :
            (() -> write_writer(out, v, schema, Symbol(codec)))
        profiled(f)
        rm(out; force=true)
    end
end
