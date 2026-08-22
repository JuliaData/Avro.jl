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
