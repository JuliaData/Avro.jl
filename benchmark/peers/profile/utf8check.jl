# Cost per string of validating short ASCII strings inside a decoded block: Base's validator on a view, against
# Avro.jl's own check (ASCII a word at a time, anything else through Base). Thread CPU time, best of 7.
# usage: julia --project=julia --startup-file=no -t 1 profile/utf8check.jl [ascii|mixed]
using Avro, Printf, Random

base(buf, p, n) = isvalid(String, view(buf, p:p+n-1))
avrojl(buf, p, n) = Avro.validutf8(buf, p, p + n - 1)

function run(f, buf, offs, lens)
    ok = 0
    for k in eachindex(offs)
        ok += f(buf, offs[k], lens[k])
    end
    return ok
end

cpu() = ccall(:clock_gettime_nsec_np, UInt64, (Cint,), 16) / 1e9
rng = Xoshiro(1)
lens = rand(rng, 5:40, 2_000_000)
offs = cumsum([1; lens[1:end-1] .+ 1])          # consecutive, one length byte apart, as in a decoded block
buf = rand(rng, UInt8('a'):UInt8('z'), offs[end] + lens[end])
if get(ARGS, 1, "ascii") == "mixed"               # one string in ten starts with a two-byte character
    for k in 1:10:length(offs)
        buf[offs[k]] = 0xc3; buf[offs[k] + 1] = 0xa9
    end
end
for f in (base, avrojl)
    @assert run(f, buf, offs, lens) == length(lens)
    t = minimum(begin c = cpu(); run(f, buf, offs, lens); cpu() - c end for _ in 1:7)
    @printf("%-7s %5.1f ns/string\n", f, 1e9 * t / length(lens))
end
