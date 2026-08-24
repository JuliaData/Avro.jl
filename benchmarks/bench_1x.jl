# Baseline for Avro.jl 1.1.2 (run with --project=<env pinned to Avro@1.1.2>, -t 1). Prints one JSON-ish line per metric.
using Avro, Tables, Pkg
const N = 1_000_000
rt = [(id=Int64(i), x=i / 7, name="name-$(i % 1000)", flag=isodd(i)) for i in 1:N]
dir = ARGS[1]
ver = Pkg.dependencies()[Base.identify_package("Avro").uuid].version
println("impl=Avro.jl version=$ver julia=$VERSION threads=$(Threads.nthreads()) cpu=$(Sys.cpu_info()[1].model) rows=$N")
Avro.writetable(joinpath(dir, "warm.avro"), rt[1:1000]); Avro.readtable(joinpath(dir, "warm.avro"))
for (label, kw) in (("null", (;)), ("zstd", (; compress=:zstd)), ("deflate", (; compress=:deflate)))
    f = joinpath(dir, "bench1x-$label.avro")
    ts = [(@elapsed Avro.writetable(f, rt; kw...)) for _ in 1:3]
    println("metric=write codec=$label best_s=$(minimum(ts)) all_s=$ts bytes=$(filesize(f))")
end
f = joinpath(dir, "bench1x-null.avro")
ts = [(@elapsed Avro.readtable(f)) for _ in 1:3]
println("metric=read_index codec=null best_s=$(minimum(ts)) all_s=$ts")
t = Avro.readtable(f)
ts = [(@elapsed Tables.columntable(t)) for _ in 1:3]
println("metric=materialize_columns codec=null best_s=$(minimum(ts)) all_s=$ts")
ts = [(@elapsed Tables.columntable(Avro.readtable(f))) for _ in 1:3]
println("metric=read_to_columns codec=null best_s=$(minimum(ts)) all_s=$ts")
f = joinpath(dir, "bench1x-zstd.avro")
ts = [(@elapsed Tables.columntable(Avro.readtable(f))) for _ in 1:3]
println("metric=read_to_columns codec=zstd best_s=$(minimum(ts)) all_s=$ts")
buf1 = Avro.write((a=1, b="x", c=1.5)); T1 = NamedTuple{(:a,:b,:c),Tuple{Int64,String,Float64}}
Avro.read(buf1, T1)
gc = Base.gc_num(); Avro.read(buf1, T1); gc2 = Base.gc_num()
println("metric=single_record_read bytes=$(@allocated Avro.read(buf1, T1)) allocs=$(Base.gc_alloc_count(Base.GC_Diff(gc2, gc)))")
gc = Base.gc_num(); Avro.write((a=1, b="x", c=1.5)); gc2 = Base.gc_num()
println("metric=single_record_write bytes=$(@allocated Avro.write((a=1, b="x", c=1.5))) allocs=$(Base.gc_alloc_count(Base.GC_Diff(gc2, gc)))")
