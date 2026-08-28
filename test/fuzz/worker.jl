# Fuzz worker (plan §9.7): mutates corpus inputs and checks the error guarantee (§4.13) and the
# acceptance-equivalence rules (§9.8) in a subprocess bounded by CPU time (setrlimit), RSS (self-check)
# and the parent's wall-clock watchdog. Invoked by test/fuzz.jl as
#   julia worker.jl <fixtures> <sample.tsv> <from> <to> <iterations> <results.tsv> <failures dir> <rss MiB> <cpu s>
# and included by it for the in-process helpers.

using Avro, Random

struct FuzzEntry
    kind::Symbol          # :datum, :schema, :json, :single
    source::String        # fixture-relative path, or "schema|data" for datum/json/single sources
    index::Int            # 1-based line of a .jsonl source; 0 otherwise
    seed::Int
end

# ---- corpus ------------------------------------------------------------------------------------------

function schemafiles(fixtures, dir)
    d = joinpath(fixtures, dir)
    return [dir * "/" * f for f in sort(readdir(d)) if endswith(f, ".avsc")]
end

"Every corpus entry in a fixed order (datums and JSON texts per generated line, schemas, single-object messages)."
function fuzzcorpus(fixtures::AbstractString)
    datums = Tuple{String,String}[]
    schemas = String[]
    for f in schemafiles(fixtures, "generated/roots")
        push!(schemas, f)
        push!(datums, (f, f[1:end - 5] * ".jsonl"))
    end
    for f in schemafiles(fixtures, "generated/schemas")
        push!(schemas, f)
        jsonl = "generated/data/" * basename(f)[1:end - 5] * ".jsonl"
        isfile(joinpath(fixtures, jsonl)) && push!(datums, (f, jsonl))
    end
    for f in schemafiles(fixtures, "generated/sortorder")
        push!(schemas, f)
        push!(datums, (f, f[1:end - 5] * ".a.json"))
        push!(datums, (f, f[1:end - 5] * ".b.json"))
    end
    for dir in ("apache", "generated/evolution", "generated/singleobject", "generated/blocking", "generated/blocking/crossform")
        append!(schemas, schemafiles(fixtures, dir))
    end
    push!(schemas, "apache/messageV1/test_schema.avsc")
    push!(datums, ("generated/blocking/arrmap.avsc", "generated/blocking/arrmap.json"))
    push!(datums, ("generated/singleobject/weather.avsc", "generated/singleobject/weather1.json"))
    entries = FuzzEntry[]
    seed = Ref(0)
    function nextseed()
        seed[] += 1
        return seed[]
    end
    for (schema, src) in datums
        n = endswith(src, ".jsonl") ? count(!isempty, readlines(joinpath(fixtures, src))) : 1
        for i in 1:n
            push!(entries, FuzzEntry(:datum, schema * "|" * src, i, nextseed()))
            push!(entries, FuzzEntry(:json, schema * "|" * src, i, nextseed()))
        end
    end
    for bin in ("arrmap-32.bin", "arrmap-64.bin", "arrmap-1024.bin")
        push!(entries, FuzzEntry(:datum, "generated/blocking/arrmap.avsc|generated/blocking/" * bin, 0, nextseed()))
    end
    for bin in ("a.positive", "a.sized", "a2.positive", "a2.sized", "b.positive", "b.sized")
        push!(entries, FuzzEntry(:datum, "generated/blocking/crossform/arr.avsc|generated/blocking/crossform/" * bin * ".bin", 0, nextseed()))
    end
    for s in schemas
        push!(entries, FuzzEntry(:schema, s, 0, nextseed()))
    end
    for f in ("generated/roots/int-null.avro", "generated/roots/string-null.avro", "generated/roots/union-null.avro",
              "generated/roots/map-snappy.avro", "generated/roots/array-deflate.avro", "generated/roots/enum-zstandard.avro",
              "generated/data/everything-null.avro", "generated/data/bench-deflate.avro", "generated/data/logical-null.avro",
              "apache/weather.avro", "apache/weather-deflate.avro", "apache/weather-snappy.avro")
        push!(entries, FuzzEntry(:container, f, 0, nextseed()))
    end
    push!(entries, FuzzEntry(:single, "generated/singleobject/weather.avsc|generated/singleobject/weather1.bin", 0, nextseed()))
    push!(entries, FuzzEntry(:single, "apache/messageV1/test_schema.avsc|apache/messageV1/test_message.bin", 0, nextseed()))
    return entries
end

"The Julia 1.10–1.12 permutation algorithm, frozen so the recorded sample is version-stable."
function recordedperm(rng::AbstractRNG, n::Int)
    permutation = Vector{Int}(undef, n)
    n == 0 && return permutation
    permutation[1] = 1
    mask = 3
    @inbounds for i in 2:n
        candidate = 0
        while true
            candidate = Int((rand(rng, UInt64) >> 12) & UInt64(mask))
            candidate < i && break
        end
        j = candidate + 1
        if i != j
            permutation[i] = permutation[j]
        end
        permutation[j] = i
        i == mask + 1 && (mask = 2 * mask + 1)
    end
    return permutation
end

"The recorded sample: `n` entries drawn by a fixed seed from the corpus order."
function fuzzsample(fixtures::AbstractString; n::Int=200)
    entries = fuzzcorpus(fixtures)
    perm = recordedperm(Xoshiro(20260822), length(entries))
    return entries[perm[1:min(n, end)]]
end

function samplefile(entries)
    return join(["$(e.kind)\t$(e.source)\t$(e.index)\t$(e.seed)" for e in entries],
                "\n") * "\n"
end

function readsample(path::AbstractString)
    out = FuzzEntry[]
    for line in eachline(path)
        isempty(line) && continue
        f = split(line, '\t')
        push!(out, FuzzEntry(Symbol(f[1]), String(f[2]), parse(Int, f[3]), parse(Int, f[4])))
    end
    return out
end

function loadentry(fixtures, e::FuzzEntry)
    e.kind === :schema && return (text=read(joinpath(fixtures, e.source), String),)
    e.kind === :container && return (bytes=read(joinpath(fixtures, e.source)),)
    schemapath, src = split(e.source, '|')
    s = Avro.parseschema(read(joinpath(fixtures, schemapath), String))
    if e.kind === :single
        store = Avro.SchemaCache()
        Avro.register!(store, s)
        return (schema=s, store=store, bytes=read(joinpath(fixtures, src)))
    end
    endswith(src, ".bin") && return (schema=s, plan=Avro.readplan(s), bytes=read(joinpath(fixtures, src)), text="")
    text = endswith(src, ".jsonl") ? filter(!isempty, readlines(joinpath(fixtures, src)))[e.index] : strip(read(joinpath(fixtures, src), String))
    return (schema=s, plan=Avro.readplan(s), bytes=Avro.encode(s, Avro.fromjson(s, text)), text=String(text))
end

# ---- mutations ---------------------------------------------------------------------------------------

const LONGSCHEMA = Avro.parseschema("\"long\"")
const EXTREMES = Int64[typemax(Int64), typemin(Int64), -1, 1 << 31, 1 << 32, 1 << 24, -(1 << 24), 0, 1, 1 << 62, typemax(Int32), typemin(Int32)]
const EXTREME_VARINTS = [Avro.encode(LONGSCHEMA, x) for x in EXTREMES]

"One seeded mutation of `bytes`: truncation at every offset first for small inputs, then a random operator."
function mutate(rng::AbstractRNG, bytes::Vector{UInt8}, iteration::Int)
    n = length(bytes)
    n <= 64 && iteration <= n && return (bytes[1:iteration - 1], :truncate)
    op = n == 0 ? 2 : rand(rng, 1:7)
    if op == 1
        m = copy(bytes)
        m[rand(rng, 1:n)] ⊻= UInt8(1) << rand(rng, 0:7)
        return (m, :bitflip)
    elseif op == 2
        i = rand(rng, 1:n + 1)
        return (vcat(bytes[1:i - 1], rand(rng, UInt8), bytes[i:end]), :insert)
    elseif op == 3
        i = rand(rng, 1:n)
        return (vcat(bytes[1:i - 1], bytes[i + 1:end]), :delete)
    elseif op == 4
        return (bytes[1:rand(rng, 0:n - 1)], :truncate)
    elseif op == 5
        i = rand(rng, 1:n)
        m = copy(bytes)
        m[i] |= 0x80                                           # a varint byte becomes a continuation byte
        return (vcat(m[1:i], rand(rng, (0x00, 0x7f, 0x80, 0xff)), m[i + 1:end]), :varint)
    elseif op == 6
        i = rand(rng, 1:n)
        vb = EXTREME_VARINTS[rand(rng, eachindex(EXTREME_VARINTS))]
        return (vcat(bytes[1:i - 1], vb, bytes[min(i + length(vb), n + 1):end]), :extreme)
    end
    m = copy(bytes)
    m[rand(rng, 1:n)] = rand(rng, UInt8)
    return (m, :overwrite)
end

# ---- verdicts ----------------------------------------------------------------------------------------

function classify(e)
    return e isa Avro.DataError ? :data : e isa Avro.LimitError ? :limit :
           e isa Avro.AvroError ? :avro : :other
end

"Run `f`; `(verdict, message)` where the verdict is `:ok`, `:data`, `:limit`, another `:avro` error, or `:other` (a guarantee failure)."
function attempt(f)
    try
        f()
        return (:ok, "")
    catch e
        return (classify(e), sprint(showerror, e))
    end
end

function skipdatum(plan, m::Vector{UInt8}, limits)
    return Avro.withbudget(limits) do b
        Avro.addinput!(b, length(m))
        d = Avro.Decoder(m, b)
        Avro.skip(plan, d)
        d.pos == length(m) + 1 || throw(Avro.DataError("trailing bytes after the datum", d.pos))
        nothing
    end
end

function projectdatum(plan, m::Vector{UInt8}, i::Int, limits)
    return Avro.withbudget(limits) do b
        Avro.addinput!(b, length(m))
        d = Avro.Decoder(m, b)
        Avro.decodecolumns(plan, d, 1; selected=[i])
        d.pos == length(m) + 1 || throw(Avro.DataError("trailing bytes after the datum", d.pos))
        nothing
    end
end

function isutf8(msg)
    return occursin("UTF-8", msg)
end

"A raw datum under its valid schema: DataError/LimitError or a valid decode, and the §9.8 acceptance rules."
function datumcase(ctx, m::Vector{UInt8}, limits)
    s, plan = ctx.schema, ctx.plan
    strict = attempt(() -> Avro.decode(s, m; limits=limits))
    strict[1] in (:ok, :data, :limit) || return "guarantee: decode raised $(strict[1]) — $(strict[2])"
    fast = attempt(() -> Avro.decode(s, m; limits=limits, validate=:fast))
    fast[1] in (:ok, :data, :limit) || return "guarantee: fast decode raised $(fast[1]) — $(fast[2])"
    strict[1] === :ok && fast[1] !== :ok && return "equivalence: fast rejects a datum strict accepts — $(fast[2])"
    sk = attempt(() -> skipdatum(plan, m, limits))
    sk[1] in (:ok, :data, :limit) || return "guarantee: skip raised $(sk[1]) — $(sk[2])"
    strict[1] === :ok && sk[1] !== :ok && return "equivalence: skip rejects a datum decode accepts — $(sk[2])"
    sk[1] === :ok && strict[1] !== :ok && !isutf8(strict[2]) && return "equivalence: skip accepts a datum decode rejects — $(strict[2])"
    if s isa Avro.RecordSchema
        for i in eachindex(s.fields)
            pr = attempt(() -> projectdatum(plan, m, i, limits))
            pr[1] in (:ok, :data, :limit) || return "guarantee: projection $i raised $(pr[1]) — $(pr[2])"
            (pr[1] === :ok) == (strict[1] === :ok) || isutf8(strict[2]) || isutf8(pr[2]) ||
                return "equivalence: projection $i is $(pr[1]) but the full decode is $(strict[1]) — $(strict[2]) / $(pr[2])"
        end
    end
    return nothing
end

"A mutated schema text: any AvroError or a schema whose canonical form, fingerprint and JSON print."
function schemacase(m::Vector{UInt8}, limits)
    parsed = Ref{Any}(nothing)
    r = attempt(() -> (parsed[] = Avro.parseschema(m; limits=limits)))
    r[1] === :other && return "guarantee: parseschema raised — $(r[2])"
    r[1] === :ok || return nothing
    for f in (Avro.canonical, Avro.fingerprint, Avro.json)
        t = attempt(() -> f(parsed[]))
        t[1] === :other && return "guarantee: $(nameof(f)) raised on a parsed schema — $(t[2])"
    end
    return nothing
end

"A mutated JSON datum: any AvroError, or a value that encodes."
function jsoncase(ctx, m::Vector{UInt8}, limits)
    v = Ref{Any}(nothing)
    r = attempt(() -> (v[] = Avro.fromjson(ctx.schema, m; limits=limits)))
    r[1] === :other && return "guarantee: fromjson raised — $(r[2])"
    r[1] === :ok || return nothing
    t = attempt(() -> Avro.encode(ctx.schema, v[]; limits=limits))
    t[1] === :ok || return "invariant: a value from fromjson does not encode — $(t[2])"
    return nothing
end

function singlecase(ctx, m::Vector{UInt8}, limits)
    r = attempt(() -> Avro.decodesingle(m, ctx.store; limits=limits))
    r[1] === :other && return "guarantee: decodesingle raised — $(r[2])"
    return nothing
end

"A mutated container file: any AvroError or a valid read (the §4.13 whole-file guarantee)."
function containercase(m::Vector{UInt8}, limits)
    r = attempt(() -> Avro.Reader(rr -> (foreach(identity, Avro.eachdatum(rr)); nothing), m; limits=limits))
    r[1] === :other && return "guarantee: container read raised $(r[1]) — $(r[2])"
    return nothing
end

function runcase(ctx, kind::Symbol, m::Vector{UInt8}, limits)
    kind === :datum && return datumcase(ctx, m, limits)
    kind === :schema && return schemacase(m, limits)
    kind === :json && return jsoncase(ctx, m, limits)
    kind === :container && return containercase(m, limits)
    return singlecase(ctx, m, limits)
end

# ---- shrinking and persistence -----------------------------------------------------------------------

"ddmin-style byte-level reduction of `bytes` while `f` keeps returning true (bounded attempts)."
function shrink(f, bytes::Vector{UInt8}; maxattempts::Int=2000)
    cur = bytes
    n = 2
    attempts = 0
    while length(cur) > 1 && attempts < maxattempts
        chunk = cld(length(cur), n)
        reduced = false
        for i in 1:n
            lo = (i - 1) * chunk + 1
            lo > length(cur) && break
            hi = min(i * chunk, length(cur))
            cand = vcat(cur[1:lo - 1], cur[hi + 1:end])
            attempts += 1
            isempty(cand) && continue
            f(cand) || continue
            cur = cand
            n = max(n - 1, 2)
            reduced = true
            break
        end
        reduced && continue
        n >= length(cur) && break
        n = min(2n, length(cur))
    end
    return cur
end

function failureclass(msg)
    return first(split(msg, " — "))
end

function persistfailure(faildir, e::FuzzEntry, iteration::Int, op::Symbol, m::Vector{UInt8}, msg::String, reproduce)
    mkpath(faildir)
    class = failureclass(msg)
    small = shrink(b -> (r = reproduce(b); r !== nothing && failureclass(r) == class), m)
    name = string(e.kind, "-", bytes2hex(Avro.SHA.sha1(small))[1:12])
    write(joinpath(faildir, name * ".bin"), small)
    write(joinpath(faildir, name * ".orig.bin"), m)
    write(joinpath(faildir, name * ".txt"), "kind: $(e.kind)\nsource: $(e.source)\nindex: $(e.index)\nseed: $(e.seed)\niteration: $iteration\noperator: $op\n$msg\n")
    return (name, small)
end

function runentry(fixtures, e::FuzzEntry, iterations::Int, limits, faildir)
    ctx = loadentry(fixtures, e)
    input = e.kind in (:schema, :json) ? Vector{UInt8}(codeunits(ctx.text)) : ctx.bytes
    rng = Xoshiro(e.seed)
    cases = 0
    failures = 0
    for it in 1:iterations
        m, op = mutate(rng, input, it)
        cases += 1
        msg = runcase(ctx, e.kind, m, limits)
        msg === nothing && continue
        failures += 1
        if failures <= 3
            name, small = persistfailure(faildir, e, it, op, m, msg, b -> runcase(ctx, e.kind, b, limits))
            println(stderr, "fuzz failure ", name, " (", e.kind, " ", e.source, " #", e.index, " iteration ", it, " ", op, "): ", msg, "\n  shrunk input: ", bytes2hex(small))
        end
    end
    return (cases=cases, failures=failures)
end

function main(args)
    length(args) == 9 || error("usage: worker.jl <fixtures> <sample.tsv> <from> <to> <iterations> <results.tsv> <failures dir> <rss MiB> <cpu s>")
    fixtures, sample, results, faildir = args[1], args[2], args[6], args[7]
    from, to, iterations, rssmib, cpus = parse.(Int, (args[3], args[4], args[5], args[8], args[9]))
    Sys.isunix() && ccall(:setrlimit, Cint, (Cint, Ptr{UInt64}), 0, UInt64[cpus, cpus + 60])   # RLIMIT_CPU
    entries = readsample(sample)[from:to]
    limits = Avro.Limits()
    return open(results, "w") do io
        for e in entries
            c = runentry(fixtures, e, iterations, limits, faildir)
            println(io, e.kind, '\t', e.source, '\t', e.index, '\t', e.seed, '\t', c.cases, '\t', c.failures)
            flush(io)
            Sys.maxrss() > rssmib << 20 && (println(stderr, "RSS limit exceeded: ", Sys.maxrss() >> 20, " MiB"); return 3)
        end
        return 0
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    exit(main(ARGS))
end
