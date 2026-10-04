# Cross-version producer (plan §12 Phase 4c/5 gate): writes a corpus of container files plus their
# expected JSON datum lines into ARGS[1]; a consumer on a different Julia version verifies them.
using Avro
outdir = abspath(ARGS[1])
mkpath(outdir)
open(joinpath(outdir, "VERSION"), "w") do io
    println(io, VERSION)
end
cases = Pair{String,Any}[]
push!(cases, "records" => ([(id=Int64(i), x=i / 7, name="n$i", flag=isodd(i), opt=i % 3 == 0 ? missing : Int64(i)) for i in 1:5000],
                           """{"type":"record","name":"CV","fields":[{"name":"id","type":"long"},{"name":"x","type":"double"},{"name":"name","type":"string"},{"name":"flag","type":"boolean"},{"name":"opt","type":["null","long"]}]}"""))
push!(cases, "longs" => ([Int64(i)^2 for i in 1:2000], "\"long\""))
push!(cases, "strings" => (["s$i" for i in 1:2000], "\"string\""))
push!(cases, "arrays" => ([Int64[i, i + 1] for i in 1:500], """{"type":"array","items":"long"}"""))
for (name, (vals, sjson)) in cases
    s = Avro.parseschema(sjson)
    for codec in (:null, :deflate, :zstandard, :snappy)
        file = joinpath(outdir, "$name-$codec.avro")
        w = Avro.Writer(file, s; codec=codec)
        foreach(v -> push!(w, v), vals)
        close(w)
    end
    open(joinpath(outdir, "$name.jsonl"), "w") do io
        for v in vals
            println(io, Avro.tojson(s, v))
        end
    end
end
println("wrote ", length(cases) * 4, " files from Julia ", VERSION)
