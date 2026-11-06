# Does filling a preallocated (already old) Vector{String} with fresh strings make each young collection
# rescan the whole vector? If so, GC time per stored string grows with the vector's length.
# usage: julia --startup-file=no -t 1 profile/gcscan.jl
using Printf

function fill_strings!(cols, n)
    for i in 1:n, col in cols
        col[i] = string(i)
    end
end

function drop_strings(ncols, n)
    s = 0
    for i in 1:n, _ in 1:ncols
        s += sizeof(string(i))
    end
    return s
end

const NCOLS = 10
for n in (250_000, 500_000, 1_000_000, 2_000_000)
    cols = [Vector{String}(undef, n) for _ in 1:NCOLS]
    GC.gc()                                   # the columns are old before they are filled, as Avro.Table's are
    kept = @timed fill_strings!(cols, n)
    cols = nothing
    GC.gc()
    dropped = @timed drop_strings(NCOLS, n)
    @printf("rows=%9d  kept: %.3f s, GC %.3f s (%5.1f ns/string)   dropped: %.3f s, GC %.3f s (%5.1f ns/string)\n",
            n, kept.time, kept.gctime, 1e9 * kept.gctime / (n * NCOLS),
            dropped.time, dropped.gctime, 1e9 * dropped.gctime / (n * NCOLS))
end
