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
    out = []
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

function javaharnesstext(dir::String, class::String, args...)
    return String(javaharness(dir, class, args...))
end

@testset "§8.5 matrix: containers, canonical, resolution, single-object, sort order, negatives" begin
    gen = joinpath(FIXTURES, "generated")
    hd = harnessdir()
    py = AVRO_PYTHON
    haspy = !isempty(py) && success(run(pipeline(`$py -c "import fastavro"`; stdout=devnull, stderr=devnull)))
    haspy || @info "fastavro oracle not available (set AVRO_PYTHON); the Python halves are skipped"
    schemacases = String[]
    for d in ("roots", "schemas", "evolution")
        for f in readdir(joinpath(gen, d); join=true)
            endswith(f, ".avsc") && push!(schemacases, f)
        end
    end
    for f in readdir(joinpath(FIXTURES, "apache"); join=true)
        endswith(f, ".avsc") && push!(schemacases, f)
    end
    # fastavro does not expose union branch identity after reading, so comparisons against that oracle
    # are value-semantic. Numeric equality remains exact.
    function looseeq(a, b)
        return isequal(a, b)
    end
    function looseeq(a::Avro.UnionValue, b)
        return looseeq(a.value, b)
    end
    function looseeq(a, b::Avro.UnionValue)
        return looseeq(a, b.value)
    end
    function looseeq(a::Avro.UnionValue, b::Avro.UnionValue)
        return looseeq(a.value, b.value)
    end
    function looseeq(a::Real, b::Real)
        return a isa Bool || b isa Bool ? isequal(a, b) : (isequal(a, b) || a == b)
    end
    function looseeq(a::Avro.EnumValue, b::AbstractString)
        return String(a) == b
    end
    function looseeq(a::AbstractString, b::Avro.EnumValue)
        return a == String(b)
    end
    function looseeq(a::AbstractVector, b::AbstractVector)
        return length(a) == length(b) && all(looseeq(x, y) for (x, y) in zip(a, b))
    end
    function looseeq(a::Avro.Record, b::Avro.Record)
        ka, kb = keys(a), keys(b)
        ka == kb || return false
        return all(looseeq(a[k], b[k]) for k in ka)
    end
    function looseeq(a::Avro.Map, b::Avro.Map)
        Set(keys(a)) == Set(keys(b)) || return false
        return all(looseeq(a[k], b[k]) for k in keys(a))
    end
    @test !looseeq(Int64(2)^53 + 1, Float64(Int64(2)^53))

    # A type-preserving value tree for the Python oracle. fastavro does not expose union branch identity,
    # so unions compare by value; numeric kinds and every numeric bit remain distinct.
    function oraclevalue(::Avro.Schema, v, j)
        return j
    end
    function oraclevalue(::Union{Avro.IntSchema,Avro.LongSchema}, v, j)
        return Dict("\$integer" => string(j))
    end
    function oraclevalue(::Union{Avro.FloatSchema,Avro.DoubleSchema}, v, j)
        return Dict("\$float" => string(reinterpret(UInt64, Float64(v)); base=16, pad=16))
    end
    function oraclevalue(::Union{Avro.BytesSchema,Avro.FixedSchema}, v, j)
        return Dict("\$bytes" => bytes2hex(UInt8[UInt8(c) for c in j]))
    end
    function oraclevalue(s::Avro.ArraySchema, v, j)
        return Any[oraclevalue(s.items, x, y) for (x, y) in zip(v, j)]
    end
    function oraclevalue(s::Avro.MapSchema, v, j)
        return Dict(k => oraclevalue(s.values, v[k], value) for (k, value) in j)
    end
    function oraclevalue(s::Avro.RecordSchema, v, j)
        return Dict(f.name => oraclevalue(f.schema, v[f.name], j[f.name]) for f in s.fields)
    end
    function oraclevalue(s::Avro.UnionSchema, v, j)
        j === nothing && return j
        i, inner = if v isa Avro.UnionValue
            (v.index, v.value)
        else
            (3 - Avro.nullablebranch(s), v)
        end
        branch = s.branches[i]
        label = Avro.unionlabel(branch)
        return oraclevalue(branch, inner, j[label])
    end
    function oraclejson(s::Avro.Schema, v)
        tree = Avro.JSON.parse(Avro.tojson(s, v))
        return Avro.JSON.json(oraclevalue(s, v, tree))
    end

    @testset "(1) Julia-written containers read by both oracles, every codec" begin
        availablecodecs = Set(Avro.codecs())
        requiredcodecs = (:null, :deflate, :snappy, :zstandard)
        @test all(in(availablecodecs), requiredcodecs)
        containercodecs = filter(in(availablecodecs), (:null, :deflate, :snappy, :zstandard, :bzip2, :xz))
        testedcodecs = Set{Symbol}()
        pycheck = tempname() * ".py"
        write(pycheck, """
import json, struct, sys, fastavro
fastavro.read.LOGICAL_READERS.clear()
inp, expectedp = sys.argv[1], sys.argv[2]
with open(inp, "rb") as f:
    r = fastavro.reader(f)
    records = list(r)
with open(expectedp, "r", encoding="utf-8") as f:
    expected = [json.loads(line) for line in f if line.strip()]
def normalise(value):
    if value is None or isinstance(value, (bool, str)): return value
    if isinstance(value, int): return {"\$integer": str(value)}
    if isinstance(value, float): return {"\$float": struct.pack(">d", value).hex()}
    if isinstance(value, (bytes, bytearray)): return {"\$bytes": bytes(value).hex()}
    if isinstance(value, dict): return {key: normalise(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)): return [normalise(item) for item in value]
    raise TypeError(f"unsupported fastavro value {type(value)!r}")
if normalise(records) != expected:
    raise AssertionError("fastavro container values differ from Julia's Avro JSON values")
""")
        for avsc in schemacases
            s = Avro.parseschema(read(avsc, String))
            jsonl = joinpath(dirname(avsc) == joinpath(gen, "roots") ? dirname(avsc) : joinpath(gen, "data"),
                             basename(avsc)[1:end - 5] * ".jsonl")
            isfile(jsonl) || continue
            vs = [Avro.fromjson(s, line) for line in filter(!isempty, readlines(jsonl))]
            isempty(vs) && continue
            for codec in containercodecs
                dir = mktempdir()
                file = joinpath(dir, "j.avro")
                w = Avro.Writer(file, s; codec=codec)
                foreach(v -> push!(w, v), vs)
                close(w)
                jout = String(javatool("tojson", file))
                back = [Avro.fromjson(s, line) for line in filter(!isempty, split(jout, '\n'))]
                @test length(back) == length(vs)                            # the datum count survives
                @test isequal(back, vs)                                     # Java reads every codec semantically
                jschema = Avro.parseschema(String(javatool("getschema", file)))
                @test Avro.parsingequivalent(jschema, s)                    # the schema survives
                jmeta = String(javatool("getmeta", file))
                @test occursin(String(codec), jmeta)                        # the codec name survives in the metadata
                if haspy
                    expected = joinpath(dir, "expected.jsonl")
                    open(expected, "w") do expectedio
                        for v in vs
                            println(expectedio, oraclejson(s, v))
                        end
                    end
                    pyerr = joinpath(dir, "fastavro.err")
                    pyproc = run(pipeline(`$py $pycheck $file $expected`; stdout=devnull, stderr=pyerr); wait=false)
                    wait(pyproc)
                    ok = success(pyproc)
                    ok || @info "fastavro semantic comparison failed" schema=basename(avsc) codec stderr=read(pyerr, String)
                    @test ok
                end
                push!(testedcodecs, codec)
            end
        end
        @test testedcodecs == Set(containercodecs)
    end
    @testset "(3) canonical form and fingerprints vs avro-tools" begin
        for avsc in schemacases
            s = Avro.parseschema(read(avsc, String))
            jc = strip(String(javatool("canonical", avsc, "-")))
            @test jc == Avro.canonical(s)
            jf = split(strip(String(javatool("fingerprint", avsc))))[1]
            @test bswap(parse(UInt64, jf; base=16)) == Avro.fingerprint(s)   # avro-tools prints the CRC little-endian
            for (javaalgorithm, juliaalgorithm) in (("MD5", :md5), ("SHA-256", :sha256))
                digest = split(strip(String(javatool("fingerprint", "--fingerprint", javaalgorithm, avsc))))[1]
                @test digest == bytes2hex(Avro.fingerprint(s; algorithm=juliaalgorithm))
            end
        end
    end
    if hd !== nothing
        @testset "(4) schema resolution vs the Java reader" begin
            pairsdir = joinpath(gen, "evolution")
            resolutionfixtures = [
                (joinpath(gen, "data", "everything-null.avro"), joinpath(pairsdir, "everything_readerA.avsc")),
                (joinpath(gen, "data", "everything-null.avro"), joinpath(pairsdir, "everything_readerB.avsc")),
                (joinpath(FIXTURES, "apache", "weather.avro"), joinpath(pairsdir, "weather_reader.avsc")),
            ]
            resolutioncases = 0
            for (data, rf) in resolutionfixtures
                @test isfile(data) && isfile(rf)
                rs = Avro.parseschema(read(rf, String))
                jout = javaharnesstext(hd, "ReadWithReader", data, rf)
                jvals = [Avro.fromjson(rs, line) for line in filter(!isempty, split(jout, '\n'))]
                resolved = Avro.Rows(data; reader_schema=rs, union_resolution=:java)
                rvals = Any[Avro.record(row) for row in resolved]
                close(resolved)
                @test !isempty(rvals)
                @test looseeq(rvals, jvals)
                resolutioncases += 1
            end
            @test resolutioncases == length(resolutionfixtures)
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
    function samplesort(ss::Avro.Schema, rng)
        ss isa Avro.RecordSchema || error("record shapes only")
        vals = Any[]
        for f in ss.fields
            fs = f.schema
            push!(vals, fs isa Avro.DoubleSchema ? rand(rng, (-0.0, 0.0, 1.5, -2.5, Inf, -Inf, NaN)) :
                        fs isa Avro.LongSchema ? rand(rng, Int64(-5):Int64(5)) :
                        fs isa Avro.StringSchema ? String(rand(rng, 'a':'c', rand(rng, 0:3))) :
                        fs isa Avro.EnumSchema ? String(rand(rng, fs.symbols)) :
                        fs isa Avro.UnionSchema ? (rand(rng, Bool) ? Avro.UnionValue(1, Int32(rand(rng, -3:3))) : Avro.UnionValue(2, String(rand(rng, 'a':'c', 2)))) :
                        error("unhandled sort shape"))
        end
        nt = NamedTuple{Tuple(Symbol(f.name) for f in ss.fields)}(Tuple(vals))
        return nt
    end

    @testset "(2b) positive and sized collection block forms as independent oracle cases" begin
        arr = Avro.parseschema("{\"type\":\"array\",\"items\":\"long\"}")
        avsc = tempname() * ".avsc"
        write(avsc, Avro.json(arr))
        positive = Avro.encode(arr, Int64[3, 4, 5])                       # the writer's positive-count form
        items = reduce(vcat, [Avro.encode(Avro.parseschema("\"long\""), Int64(v)) for v in (3, 4, 5)])
        sized = vcat(Avro.encode(Avro.parseschema("\"long\""), -3)[1:0], UInt8[0x05], UInt8[UInt8(2 * length(items))], items, UInt8[0x00])
        # zigzag(-3) = 5 = 0x05; the sized form declares its byte size after the negative count
        for (label, bytes) in (("positive", positive), ("sized", sized))
            f = tempname()
            write(f, bytes)
            jout = strip(String(javatool("fragtojson", "--no-pretty", "--schema-file", avsc, f)))
            @test Avro.fromjson(arr, jout) == Int64[3, 4, 5]              # Java decodes both wire forms
            @test Avro.decode(arr, bytes) == Int64[3, 4, 5]               # and so does Julia
        end
    end
    @testset "(4b) resolution: both policies, both oracles, constructed pairs" begin
        wsrc = "{\"type\":\"record\",\"name\":\"RP\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"old\",\"type\":\"string\"},{\"name\":\"u\",\"type\":[\"null\",\"long\"]}]}"
        rsrc = "{\"type\":\"record\",\"name\":\"RP\",\"fields\":[{\"name\":\"a\",\"type\":\"double\"},{\"name\":\"renamed\",\"type\":\"string\",\"aliases\":[\"old\"]},{\"name\":\"u\",\"type\":[\"null\",\"long\"]},{\"name\":\"z\",\"type\":\"int\",\"default\":5}]}"
        ws, rs = Avro.parseschema(wsrc), Avro.parseschema(rsrc)
        rows = [(a=Int32(i), old="o$i", u=isodd(i) ? Int64(i) : missing) for i in 1:50]
        dir = mktempdir()
        data = joinpath(dir, "rp.avro")
        wtr = Avro.Writer(data, ws)
        foreach(v -> push!(wtr, v), rows)
        close(wtr)
        rf = joinpath(dir, "rp-reader.avsc")
        write(rf, rsrc)
        for policy in (:spec, :java)
            resolved = Avro.Rows(data; reader_schema=rs, union_resolution=policy)
            vals = Any[Avro.record(row) for row in resolved]
            close(resolved)
            @test length(vals) == 50 && vals[1].a === 1.0 && vals[1].renamed == "o1" && vals[1].z === Int32(5)
            if policy === :java && hd !== nothing
                jout = javaharnesstext(hd, "ReadWithReader", data, rf)
                jvals = [Avro.fromjson(rs, line) for line in filter(!isempty, split(jout, '\n'))]
                @test isequal([Avro.tojson(rs, v) for v in vals], [Avro.tojson(rs, v) for v in jvals])
            end
        end
        if haspy
            pyres = tempname() * ".py"
            write(pyres, """
import sys, json, fastavro
data, readerf = sys.argv[1], sys.argv[2]
reader_schema = json.load(open(readerf))
with open(data, "rb") as f:
    for rec in fastavro.reader(f, reader_schema=reader_schema):
        rec["u"] = None if rec["u"] is None else {"long": rec["u"]}   # fastavro strips union wrapping; restore Avro JSON encoding
        print(json.dumps(rec, sort_keys=True))
""")
            out = read(`$py $pyres $data $rf`, String)
            plines = filter(!isempty, split(out, '\n'))
            @test length(plines) == 50                                    # fastavro resolves the same pairs
            first = Avro.fromjson(rs, plines[1])
            @test first.a === 1.0 && first.renamed == "o1" && first.z === Int32(5)
        end
    end
    if hd !== nothing
        @testset "(6b) sort order: committed Java vectors run live; diverse shapes" begin
            sortdir = joinpath(gen, "sortorder")
            ran = 0
            for line in filter(!isempty, readlines(joinpath(sortdir, "verdicts.tsv")))
                name, _, expectbytes = split(line, '\t')
                av = joinpath(sortdir, "$name.avsc")
                fa = joinpath(sortdir, "$name.a.json")
                fb = joinpath(sortdir, "$name.b.json")
                (isfile(av) && isfile(fa) && isfile(fb)) || continue
                ss = Avro.parseschema(read(av, String))
                # compare the same wire bytes Java compared: fromjson/encode would canonicalise
                # logical surface forms (non-minimal decimal, mixed-case uuid) and mask deviations
                wa = Vector{UInt8}(javatool("jsontofrag", "--schema-file", av, fa))
                wb = Vector{UInt8}(javatool("jsontofrag", "--schema-file", av, fb))
                if startswith(expectbytes, "ERROR")
                    jok = try
                        javaharnesstext(hd, "CompareBytes", av, fa, fb)
                        true
                    catch
                        false
                    end
                    @test !jok                                            # Java rejects live, as recorded (maps have no sort order)
                    @test_throws ArgumentError Avro.comparebytes(ss, wa, wb)
                    ran += 1
                    continue
                end
                jv = parse(Int, strip(javaharnesstext(hd, "CompareBytes", av, fa, fb)))
                @test jv == parse(Int, expectbytes)                       # the committed vectors reproduce live
                @test sign(jv) == sign(Avro.comparebytes(ss, wa, wb))
                ran += 1
            end
            @test ran >= 40
            shapes = ("{\"type\":\"record\",\"name\":\"S1\",\"fields\":[{\"name\":\"d\",\"type\":\"double\"}]}",
                      "{\"type\":\"record\",\"name\":\"S2\",\"fields\":[{\"name\":\"k\",\"type\":\"long\",\"order\":\"descending\"},{\"name\":\"s\",\"type\":\"string\"}]}",
                      "{\"type\":\"record\",\"name\":\"S3\",\"fields\":[{\"name\":\"e\",\"type\":{\"type\":\"enum\",\"name\":\"EE6\",\"symbols\":[\"z\",\"a\",\"m\"]}}]}",
                      "{\"type\":\"record\",\"name\":\"S4\",\"fields\":[{\"name\":\"u\",\"type\":[\"int\",\"string\"]}]}")
            rng6 = Random.Xoshiro(20260824)
            for shape in shapes
                ss = Avro.parseschema(shape)
                av = tempname() * ".avsc"
                write(av, Avro.json(ss))
                for _ in 1:10
                    va, vb = samplesort(ss, rng6), samplesort(ss, rng6)
                    fa, fb = tempname() * ".json", tempname() * ".json"
                    write(fa, Avro.tojson(ss, va))
                    write(fb, Avro.tojson(ss, vb))
                    jv = parse(Int, strip(javaharnesstext(hd, "CompareBytes", av, fa, fb)))
                    @test sign(jv) == sign(Avro.comparebytes(ss, Avro.encode(ss, va), Avro.encode(ss, vb)))
                end
            end
        end
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
            @test jv == (i in javatolerated)                                # each recorded tolerance occurs, exactly
            pv === nothing || @test pv == (i in pytolerated)
        end
        # malformed schemas: each oracle's parse verdict, two-way
        badschemas = [
            ("truncated json", "{\"type\":\"record\",\"name\":\"B\""),
            ("duplicate field", "{\"type\":\"record\",\"name\":\"B\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"a\",\"type\":\"int\"}]}"),
            ("invalid name", "{\"type\":\"record\",\"name\":\"9bad\",\"fields\":[]}"),
            ("unknown type", "{\"type\":\"wibble\"}"),
        ]
        for (label, src) in badschemas
            uok = try
                Avro.parseschema(src)
                true
            catch
                false
            end
            @test !uok
            f = tempname() * ".avsc"
            write(f, src)
            jok = try
                javatool("canonical", f, "-")
                true
            catch
                false
            end
            @test !jok                                                      # Java rejects each malformed schema
        end
        # malformed raw datums: fragtojson verdicts, two-way
        ls = Avro.parseschema("\"string\"")
        lavsc = tempname() * ".avsc"
        write(lavsc, "\"string\"")
        baddatums = [
            ("negative length", UInt8[0x01]),
            ("truncated payload", UInt8[0x06, 0x61]),
            ("overlong length", vcat(UInt8[0xac, 0x02], fill(UInt8('a'), 3))),
        ]
        for (label, bytes) in baddatums
            uok = try
                Avro.decode(ls, bytes)
                true
            catch
                false
            end
            @test !uok
            f = tempname()
            write(f, bytes)
            jok = try
                javatool("fragtojson", "--schema-file", lavsc, f)
                true
            catch
                false
            end
            @test !jok                                                      # Java rejects each malformed datum
        end
        # malformed JSON datums: jsontofrag verdicts, two-way
        badjson = [("bare word", "notjson"), ("wrong type", "{\"a\": 1}"), ("trailing", "\"x\" garbage")]
        for (label, txt) in badjson
            uok = try
                Avro.fromjson(ls, txt)
                true
            catch
                false
            end
            @test !uok
            f = tempname() * ".json"
            write(f, txt)
            jok = try
                javatool("jsontofrag", "--schema-file", lavsc, f)
                true
            catch
                false
            end
            @test !jok                                                      # Java rejects each malformed JSON datum
        end
        jv, uv, pv = verdicts(good)
        @test jv && uv && (pv === nothing || pv)                            # and all accept the valid file
    end
end
