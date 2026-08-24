# Live differential tests against Apache avro-tools (plan §8.5, Phase 2 datum level): run with
# AVRO_INTEROP=true and AVRO_TOOLS_JAR pointing at the pinned avro-tools-1.12.2.jar (sha256
# 6220e8bc089aaf917cdad4cd358bd651fc0394c0e5ddb8b36da402012c294a68) with a JDK ≥ 17 on PATH. Java's
# `tojson` corpus lines are decoded by Julia (`fromjson`) and re-encoded: byte-exact against `jsontofrag`
# for deterministic schemas (no arrays or maps), semantic in both directions otherwise (Julia decodes
# Java's bytes; Java's `fragtojson --no-pretty` of Julia's bytes decodes to the same values).

const AVRO_TOOLS_JAR = get(ENV, "AVRO_TOOLS_JAR", "")

"Run avro-tools with stdin closed (a tool falling back to stdin must fail, not hang) and a 120 s watchdog."
function javatool(args...)
    outpath, errpath = tempname(), tempname()
    cmd = `java --enable-native-access=ALL-UNNAMED -jar $AVRO_TOOLS_JAR $args`
    p = run(pipeline(cmd; stdin=devnull, stdout=outpath, stderr=errpath); wait=false)
    t0 = time()
    while process_running(p) && time() - t0 < 120
        sleep(0.05)
    end
    process_running(p) && (kill(p); wait(p); error("avro-tools $(join(args, ' ')) timed out"))
    success(p) || error("avro-tools $(join(args, ' ')) failed: $(read(errpath, String))")
    return read(outpath)
end

function deterministic(s::Avro.Schema, seen=Set{UInt}())
    s isa Union{Avro.ArraySchema,Avro.MapSchema} && return false
    s isa Avro.UnionSchema && return all(b -> deterministic(b, seen), s.branches)
    if s isa Avro.RecordSchema
        objectid(s) in seen && return true
        push!(seen, objectid(s))
        return all(f -> deterministic(f.schema, seen), s.fields)
    end
    return true
end

function decodeall(s::Avro.Schema, bytes::Vector{UInt8})
    out = Any[]
    pos = 1
    while pos <= length(bytes)
        v, pos = Avro.decode(s, bytes, pos)
        push!(out, v)
    end
    return out
end

@testset "Apache avro-tools differential (datums)" begin
    @test isfile(AVRO_TOOLS_JAR)
    @test bytes2hex(open(Avro.SHA.sha256, AVRO_TOOLS_JAR)) == "6220e8bc089aaf917cdad4cd358bd651fc0394c0e5ddb8b36da402012c294a68"
    gen = joinpath(FIXTURES, "generated")
    cases = Pair{String,String}[]
    for f in readdir(joinpath(gen, "roots"); join=true)
        endswith(f, ".avsc") && push!(cases, f => f[1:end - 5] * ".jsonl")
    end
    for f in readdir(joinpath(gen, "schemas"); join=true)
        jsonl = joinpath(gen, "data", basename(f)[1:end - 5] * ".jsonl")
        isfile(jsonl) && push!(cases, f => jsonl)
    end
    @test length(cases) >= 18
    for (avsc, jsonl) in cases
        s = Avro.parseschema(read(avsc, String))
        lines = filter(!isempty, readlines(jsonl))
        vs = [Avro.fromjson(s, line) for line in lines]
        julia = [Avro.encode(s, v) for v in vs]
        @test all(isequal(Avro.fromjson(s, Avro.tojson(s, v)), v) for v in vs)
        if Avro.minsize(s) == 0
            @test all(isempty, julia)             # zero-byte datums: avro-tools' fragtojson prints nothing for them and jsontofrag hangs on `{}` (oracle limitation, plan §8.4)
            continue
        end
        javabytes = javatool("jsontofrag", "--schema-file", avsc, jsonl)
        if deterministic(s)
            @test reduce(vcat, julia; init=UInt8[]) == javabytes
        else
            @test all(isequal.(decodeall(s, javabytes), vs))      # Julia decodes Java's encoding (block forms differ)
        end
        mktemp() do path, io
            write(io, reduce(vcat, julia; init=UInt8[]))
            close(io)
            out = isempty(vs) ? "" : String(javatool("fragtojson", "--no-pretty", "--schema-file", avsc, path))
            back = [Avro.fromjson(s, line) for line in filter(!isempty, split(out, '\n'))]
            @test all(isequal.(back, vs))                          # Java decodes Julia's encoding
        end
    end
end

# ---- the complete §8.5 matrix (Phase 5 / review round 1, R12) ---------------------------------------

const AVRO_PYTHON = get(ENV, "AVRO_PYTHON", "")

"Compile the Java harness once (classpath = the pinned jar); `nothing` when javac is unavailable."
function harnessdir()
    javac = Sys.which("javac")
    javac === nothing && return nothing
    dir = mktempdir()
    src = readdir(joinpath(@__DIR__, "interop", "java"); join=true)
    run(pipeline(`$javac -cp $AVRO_TOOLS_JAR -d $dir $(filter(f -> endswith(f, ".java"), src))`; stdout=devnull, stderr=devnull))
    return dir
end

function javaharness(dir::String, class::String, args...)
    outpath, errpath = tempname(), tempname()
    sep = Sys.iswindows() ? ";" : ":"
    cmd = `java --enable-native-access=ALL-UNNAMED -cp $AVRO_TOOLS_JAR$sep$dir $class $args`
    p = run(pipeline(cmd; stdin=devnull, stdout=outpath, stderr=errpath); wait=false)
    t0 = time()
    while process_running(p) && time() - t0 < 120
        sleep(0.05)
    end
    process_running(p) && (kill(p); wait(p); error("harness $class timed out"))
    success(p) || error("harness $class failed: $(read(errpath, String))")
    return read(outpath)
end

javaharnesstext(dir::String, class::String, args...) = String(javaharness(dir, class, args...))

@testset "§8.5 matrix: containers, canonical, resolution, single-object, sort order, negatives" begin
    gen = joinpath(FIXTURES, "generated")
    hd = harnessdir()
    py = AVRO_PYTHON
    haspy = !isempty(py) && success(run(pipeline(`$py -c "import fastavro"`; stdout=devnull, stderr=devnull)))
    haspy || @info "fastavro oracle not available (set AVRO_PYTHON); the Python halves are skipped"
    schemacases = String[]
    for d in ("roots", "schemas")
        for f in readdir(joinpath(gen, d); join=true)
            endswith(f, ".avsc") && push!(schemacases, f)
        end
    end
    # fastavro re-encodes union values under its own branch selection (enum -> string, int/float ->
    # wider branches; recorded in Phase 4a), so its round trip compares semantically: union identity
    # unwrapped and numeric branches promoted.
    looseeq(a, b) = isequal(a, b)
    looseeq(a::Avro.UnionValue, b) = looseeq(a.value, b)
    looseeq(a, b::Avro.UnionValue) = looseeq(a, b.value)
    looseeq(a::Avro.UnionValue, b::Avro.UnionValue) = looseeq(a.value, b.value)
    looseeq(a::Real, b::Real) = a isa Bool || b isa Bool ? isequal(a, b) : (isequal(a, b) || Float64(a) == Float64(b))
    looseeq(a::Avro.EnumValue, b::AbstractString) = String(a) == b
    looseeq(a::AbstractString, b::Avro.EnumValue) = a == String(b)
    looseeq(a::AbstractVector, b::AbstractVector) = length(a) == length(b) && all(looseeq(x, y) for (x, y) in zip(a, b))
    function looseeq(a::Avro.Record, b::Avro.Record)
        ka, kb = keys(a), keys(b)
        ka == kb || return false
        return all(looseeq(a[k], b[k]) for k in ka)
    end
    function looseeq(a::Avro.Map, b::Avro.Map)
        Set(keys(a)) == Set(keys(b)) || return false
        return all(looseeq(a[k], b[k]) for k in keys(a))
    end

    @testset "(1) Julia-written containers read by both oracles, every codec" begin
        pyreencode = tempname() * ".py"
        write(pyreencode, """
import sys, fastavro
inp, outp = sys.argv[1], sys.argv[2]
with open(inp, "rb") as f:
    r = fastavro.reader(f)
    records = list(r)
    schema = r.writer_schema
with open(outp, "wb") as f:
    fastavro.writer(f, schema, records, codec="null")
""")
        for avsc in schemacases
            s = Avro.parseschema(read(avsc, String))
            jsonl = joinpath(dirname(avsc) == joinpath(gen, "roots") ? dirname(avsc) : joinpath(gen, "data"),
                             basename(avsc)[1:end - 5] * ".jsonl")
            isfile(jsonl) || continue
            vs = [Avro.fromjson(s, line) for line in filter(!isempty, readlines(jsonl))]
            isempty(vs) && continue
            for codec in (:null, :deflate, :snappy, :zstandard)
                dir = mktempdir()
                file = joinpath(dir, "j.avro")
                w = Avro.Writer(file, s; codec=codec)
                foreach(v -> push!(w, v), vs)
                close(w)
                jout = String(javatool("tojson", file))
                back = [Avro.fromjson(s, line) for line in filter(!isempty, split(jout, '\n'))]
                @test isequal(back, vs)                                     # Java reads every codec semantically
                if haspy
                    reenc = joinpath(dir, "p.avro")
                    ok = success(run(pipeline(`$py $pyreencode $file $reenc`; stdout=devnull, stderr=devnull)))
                    @test ok
                    ok && @test looseeq(collect(Avro.eachdatum(Avro.Reader(reenc))), vs)   # fastavro round-trips the values (semantic)
                end
            end
        end
    end
    @testset "(3) canonical form and fingerprints vs avro-tools" begin
        for avsc in schemacases
            s = Avro.parseschema(read(avsc, String))
            jc = strip(String(javatool("canonical", avsc, "-")))
            @test jc == Avro.canonical(s)
            jf = split(strip(String(javatool("fingerprint", avsc))))[1]
            @test bswap(parse(UInt64, jf; base=16)) == Avro.fingerprint(s)   # avro-tools prints the CRC little-endian
        end
    end
    if hd !== nothing
        @testset "(4) schema resolution vs the Java reader" begin
            pairsdir = joinpath(gen, "evolution")
            if isdir(pairsdir)
                for wf in filter(f -> endswith(f, ".writer.avsc"), readdir(pairsdir; join=true))
                    rf = replace(wf, ".writer.avsc" => ".reader.avsc")
                    data = replace(wf, ".writer.avsc" => ".avro")
                    (isfile(rf) && isfile(data)) || continue
                    ws, rs = Avro.parseschema(read(wf, String)), Avro.parseschema(read(rf, String))
                    jout = javaharnesstext(hd, "ReadWithReader", data, rf)
                    jvals = [Avro.fromjson(rs, line) for line in filter(!isempty, split(jout, '\n'))]
                    t = Avro.Reader(data; limits=Avro.Limits()) do r
                        collect(Avro.eachdatum(r))
                    end
                    resolved = Avro.Rows(data; reader_schema=rs, union_resolution=:java)
                    rvals = Any[Avro.record(row) for row in resolved]
                    close(resolved)
                    @test isequal([Avro.tojson(rs, v) for v in rvals], [Avro.tojson(rs, v) for v in jvals])
                end
            end
        end
        @testset "(5) single-object bytes cross-decoded" begin
            s = Avro.parseschema("{\"type\":\"record\",\"name\":\"SO\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"string\"}]}")
            avsc = tempname() * ".avsc"
            write(avsc, Avro.json(s))
            msg = Avro.encodesingle(s, (a=Int64(42), b="xyz"))
            msgfile = tempname()
            write(msgfile, msg)
            jout = strip(javaharnesstext(hd, "SingleObject", "decode", avsc, msgfile))
            @test isequal(Avro.fromjson(s, jout), Avro.decode(s, msg[11:end]))
            datumjson = tempname() * ".json"
            write(datumjson, "{\"a\": 42, \"b\": \"xyz\"}")
            jbytes = javaharness(hd, "SingleObject", "encode", avsc, datumjson)
            store = Avro.SchemaCache()
            Avro.register!(store, s)
            @test Avro.decodesingle(jbytes, store).a == 42                  # Java's framing decodes
        end
        @testset "(6) sort-order verdicts, live" begin
            s = Avro.parseschema("{\"type\":\"record\",\"name\":\"SS\",\"fields\":[{\"name\":\"k\",\"type\":\"long\"},{\"name\":\"s\",\"type\":\"string\"}]}")
            avsc = tempname() * ".avsc"
            write(avsc, Avro.json(s))
            rng = Random.Xoshiro(20260823)
            for _ in 1:25
                a = (k=rand(rng, Int64), s=String(rand(rng, 'a':'z', rand(rng, 0:12))))
                b = (k=rand(rng, Int64), s=String(rand(rng, 'a':'z', rand(rng, 0:12))))
                fa, fb = tempname() * ".json", tempname() * ".json"
                write(fa, Avro.tojson(s, a))
                write(fb, Avro.tojson(s, b))
                jv = parse(Int, strip(javaharnesstext(hd, "CompareBytes", avsc, fa, fb)))
                @test sign(jv) == sign(Avro.comparebytes(s, Avro.encode(s, a), Avro.encode(s, b)))
            end
        end
    else
        @info "javac unavailable; the harness categories (4)-(6) are skipped"
    end
    @testset "(7) negative oracles: consensus rejections" begin
        s = Avro.parseschema("{\"type\":\"record\",\"name\":\"NG\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"boolean\"}]}")
        good = take!(Avro.tobuffer([(a=Int64(1), b=true)]; schema=s))
        function verdicts(bytes)
            f = tempname()
            write(f, bytes)
            java = try
                javatool("tojson", f)
                true
            catch
                false
            end
            julia = try
                Avro.Reader(r -> collect(Avro.eachdatum(r)), IOBuffer(bytes))
                true
            catch
                false
            end
            pyok = if haspy
                p = run(pipeline(`$py -c "import fastavro, sys; list(fastavro.reader(open(sys.argv[1], 'rb')))" $f`; stdout=devnull, stderr=devnull); wait=false)
                wait(p)
                success(p)
            else
                nothing
            end
            return (java, julia, pyok)
        end
        entries = Avro.Reader(r -> Avro.prescanblocks(r).entries, IOBuffer(good))
        cases = Vector{UInt8}[]
        push!(cases, good[1:end - 3])                                       # truncated final sync
        bad = copy(good)
        bad[entries[1].offset + 1] = 0x07                                   # invalid boolean byte
        push!(cases, bad)
        badmagic = copy(good)
        badmagic[1] = 0x58                                              # 'X': not the container magic
        push!(cases, badmagic)
        badsync = copy(good)
        badsync[end] ⊻= 0x01
        push!(cases, badsync)
        # recorded oracle verdicts (spec-justified deviations, plan §8.5(7)): both oracles accept a
        # truncated final sync (they stop at the datum count) and undomained bool bytes (framing-only
        # skipping); Julia rejects all four per the strict container contract.
        javatolerated = (1, 2)                                              # truncated final sync; undomained bool bytes
        pytolerated = (2, 3)                                                # undomained bool bytes; the magic's first byte
        for (i, c) in enumerate(cases)
            jv, uv, pv = verdicts(c)
            @test !uv                                                       # Julia rejects each malformed case
            @test !jv || i in javatolerated
            pv === nothing || @test !pv || i in pytolerated
        end
        jv, uv, pv = verdicts(good)
        @test jv && uv && (pv === nothing || pv)                            # and all accept the valid file
    end
end
