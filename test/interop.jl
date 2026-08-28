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

"Read one container metadata value with avro-tools' positional input before its option."
function javameta(file::AbstractString, key::AbstractString)
    return strip(String(javatool("getmeta", file, "--key", key)))
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
const INTEROP_FIXTURES = joinpath(FIXTURES, "interop")

"Read a whitespace-separated hexadecimal fixture."
function fixturebytes(path::AbstractString)
    return hex2bytes(filter(c -> !isspace(c), read(path, String)))
end

function fixtureverdict(x::AbstractString)
    x == "accept" && return true
    x == "reject" && return false
    return error("invalid verdict $x")
end

"The recorded live-oracle verdicts keyed by `(category, case)`."
function negativeverdicts()
    out = Dict{Tuple{String,String},NamedTuple{(:julia, :java, :fastavro, :note),Tuple{Bool,Bool,Bool,String}}}()
    path = joinpath(INTEROP_FIXTURES, "negative", "verdicts.tsv")
    for line in readlines(path)
        (isempty(line) || startswith(line, "#")) && continue
        fields = split(line, '\t'; limit=6)
        length(fields) == 6 || error("invalid negative-verdict row: $line")
        category, name, julia, java, fastavro, note = fields
        out[(category, name)] = (julia=fixtureverdict(julia), java=fixtureverdict(java),
                                 fastavro=fixtureverdict(fastavro), note=note)
    end
    return out
end

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
    hd0 = harnessdir()
    @test hd0 !== nothing
    hd0 === nothing && error("javac is required for the complete §8.5 matrix")
    hd = hd0::String
    py0 = Sys.which(AVRO_PYTHON)
    @test py0 !== nothing
    py0 === nothing && error("AVRO_PYTHON must name the pinned fastavro environment")
    py = py0::String
    haspy = success(run(pipeline(`$py -c "import fastavro"`; stdout=devnull, stderr=devnull)))
    @test haspy
    haspy || error("AVRO_PYTHON cannot import fastavro")
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
        containercodecs = (:null, :deflate, :snappy, :zstandard, :bzip2, :xz)
        @test all(in(Set(Avro.codecs())), containercodecs)
        testedcodecs = Set{Symbol}()
        pycheck = tempname() * ".py"
        write(pycheck, """
import json, struct, sys, fastavro
fastavro.read.LOGICAL_READERS.clear()
inp, expectedp, schemap, codec, suite, caseid = sys.argv[1:]
with open(inp, "rb") as f:
    r = fastavro.reader(f)
    records = list(r)
    metadata = r.metadata
    actual_codec = r.codec
with open(expectedp, "r", encoding="utf-8") as f:
    expected = [json.loads(line) for line in f if line.strip()]
with open(schemap, "r", encoding="utf-8") as f:
    expected_schema = json.load(f)
def normalise(value):
    if value is None or isinstance(value, (bool, str)): return value
    if isinstance(value, int): return {"\$integer": str(value)}
    if isinstance(value, float): return {"\$float": struct.pack(">d", value).hex()}
    if isinstance(value, (bytes, bytearray)): return {"\$bytes": bytes(value).hex()}
    if isinstance(value, dict): return {key: normalise(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)): return [normalise(item) for item in value]
    raise TypeError(f"unsupported fastavro value {type(value)!r}")
if len(records) != len(expected):
    raise AssertionError(f"fastavro datum count {len(records)} != {len(expected)}")
if normalise(records) != expected:
    raise AssertionError("fastavro container values differ from Julia's Avro JSON values")
if json.loads(metadata["avro.schema"]) != expected_schema:
    raise AssertionError("fastavro header schema differs from Julia's schema")
if metadata.get("avro.codec") != codec or actual_codec != codec:
    raise AssertionError(f"fastavro codec metadata/read state differ: {metadata.get('avro.codec')!r}/{actual_codec!r}")
if metadata.get("interop.suite") != suite or metadata.get("interop.case") != caseid:
    raise AssertionError("fastavro user metadata differs")
""")
        for avsc in schemacases
            s = Avro.parseschema(read(avsc, String))
            jsonl = joinpath(dirname(avsc) == joinpath(gen, "roots") ? dirname(avsc) : joinpath(gen, "data"),
                             basename(avsc)[1:end - 5] * ".jsonl")
            isfile(jsonl) || continue
            vs = [Avro.fromjson(s, line) for line in filter(!isempty, readlines(jsonl))]
            isempty(vs) && continue
            caseid = splitext(basename(avsc))[1]
            suiteid = "D08-category-1"
            metadata = Dict("interop.suite" => Vector{UInt8}(suiteid),
                            "interop.case" => Vector{UInt8}(caseid))
            for codec in containercodecs
                dir = mktempdir()
                file = joinpath(dir, "j.avro")
                w = Avro.Writer(file, s; codec=codec, metadata=metadata)
                foreach(v -> push!(w, v), vs)
                close(w)
                jout = String(javatool("tojson", file))
                back = [Avro.fromjson(s, line) for line in filter(!isempty, split(jout, '\n'))]
                @test length(back) == length(vs)                            # the datum count survives
                @test isequal(back, vs)                                     # Java reads every codec semantically
                jschema = Avro.parseschema(String(javatool("getschema", file)))
                @test jschema == s                                          # defaults, props and logical attributes survive
                @test javameta(file, "avro.codec") == String(codec)
                @test javameta(file, "interop.suite") == suiteid
                @test javameta(file, "interop.case") == caseid
                expected = joinpath(dir, "expected.jsonl")
                open(expected, "w") do expectedio
                    for v in vs
                        println(expectedio, oraclejson(s, v))
                    end
                end
                expectedschema = joinpath(dir, "expected.avsc")
                write(expectedschema, Avro.json(s))
                pyerr = joinpath(dir, "fastavro.err")
                pyproc = run(pipeline(`$py $pycheck $file $expected $expectedschema $(String(codec)) $suiteid $caseid`;
                                      stdout=devnull, stderr=pyerr); wait=false)
                wait(pyproc)
                ok = success(pyproc)
                ok || @info "fastavro complete container comparison failed" schema=basename(avsc) codec stderr=read(pyerr, String)
                @test ok
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

    function samplesort(ss::Avro.Schema, rng)
        ss isa Avro.RecordSchema || error("record shapes only")
        vals = []
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
        collections = joinpath(INTEROP_FIXTURES, "collections")
        pycollection = tempname() * ".py"
        write(pycollection, """
import io, json, sys, fastavro
schemaf, expectedf, hexf = sys.argv[1:]
with open(schemaf, "r", encoding="utf-8") as f:
    schema = json.load(f)
with open(expectedf, "r", encoding="utf-8") as f:
    expected = json.load(f)
with open(hexf, "r", encoding="ascii") as f:
    raw = bytes.fromhex(f.read())
stream = io.BytesIO(raw)
actual = fastavro.schemaless_reader(stream, schema)
if stream.read() != b"":
    raise AssertionError("fastavro did not consume the complete collection datum")
if actual != expected:
    raise AssertionError(f"fastavro collection value differs: {actual!r} != {expected!r}")
""")
        covered = Set{Tuple{String,String}}()
        for line in readlines(joinpath(collections, "cases.tsv"))
            (isempty(line) || startswith(line, "#")) && continue
            name, schemafile, expectedfile, positivefile, sizedfile = split(line, '\t')
            avsc = joinpath(collections, schemafile)
            expectedpath = joinpath(collections, expectedfile)
            s = Avro.parseschema(read(avsc, String))
            expected = Avro.fromjson(s, read(expectedpath, String))
            forms = (("positive", positivefile), ("sized", sizedfile))
            @test fixturebytes(joinpath(collections, positivefile)) != fixturebytes(joinpath(collections, sizedfile))
            for (form, hexfile) in forms
                hexpath = joinpath(collections, hexfile)
                bytes = fixturebytes(hexpath)
                f = tempname()
                write(f, bytes)
                jout = strip(String(javatool("fragtojson", "--no-pretty", "--schema-file", avsc, f)))
                @test looseeq(Avro.fromjson(s, jout), expected)            # Java reads this exact block form
                @test looseeq(Avro.decode(s, bytes), expected)             # Julia reads this exact block form
                pyproc = run(pipeline(`$py $pycollection $avsc $expectedpath $hexpath`;
                                      stdout=devnull, stderr=devnull); wait=false)
                wait(pyproc)
                @test success(pyproc)                                      # fastavro reads this exact block form
                push!(covered, (name, form))
            end
        end
        @test covered == Set((("array", "positive"), ("array", "sized"),
                              ("map", "positive"), ("map", "sized")))
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
        jout = javaharnesstext(hd, "ReadWithReader", data, rf)
        jvals = [Avro.fromjson(rs, line) for line in filter(!isempty, split(jout, '\n'))]
        pyres = tempname() * ".py"
        write(pyres, """
import sys, json, fastavro
data, readerf = sys.argv[1], sys.argv[2]
with open(readerf, "r", encoding="utf-8") as f:
    reader_schema = json.load(f)
with open(data, "rb") as f:
    records = list(fastavro.reader(f, reader_schema=reader_schema))
for rec in records:
    rec["u"] = None if rec["u"] is None else {"long": rec["u"]}   # restore Avro JSON union wrapping
    print(json.dumps(rec, sort_keys=True))
""")
        pyout = read(`$py $pyres $data $rf`, String)
        pvals = [Avro.fromjson(rs, line) for line in filter(!isempty, split(pyout, '\n'))]
        @test length(jvals) == length(rows) && length(pvals) == length(rows)
        compared = Set{Tuple{Symbol,Symbol}}()
        for policy in (:spec, :java)
            resolved = Avro.Rows(data; reader_schema=rs, union_resolution=policy)
            vals = Any[Avro.record(row) for row in resolved]
            close(resolved)
            @test length(vals) == length(rows)
            for (i, value) in enumerate(vals)
                @test value.a === Float64(rows[i].a)
                @test value.renamed == rows[i].old
                @test value.u === rows[i].u
                @test value.z === Int32(5)
            end
            for (oracle, oraclevals) in ((:java, jvals), (:fastavro, pvals))
                @test looseeq(vals, oraclevals)                            # compare every resolved datum
                push!(compared, (policy, oracle))
            end
        end
        @test compared == Set(((:spec, :java), (:spec, :fastavro),
                               (:java, :java), (:java, :fastavro)))
    end
    @testset "(6b) sort order: committed Java vectors run live; diverse shapes" begin
            function livecompare(class::String, av::String, fa::String, fb::String)
                try
                    return string(parse(Int, strip(javaharnesstext(hd, class, av, fa, fb))))
                catch err
                    message = sprint(showerror, err)
                    matched = match(r"Exception in thread \"main\" ([A-Za-z0-9_.]+)", message)
                    matched === nothing && rethrow()
                    return "ERROR:" * String(matched.captures[1])
                end
            end
            sortdir = joinpath(gen, "sortorder")
            ran = 0
            for line in filter(!isempty, readlines(joinpath(sortdir, "verdicts.tsv")))
                name, expectobject, expectbytes = split(line, '\t')
                av = joinpath(sortdir, "$name.avsc")
                fa = joinpath(sortdir, "$name.a.json")
                fb = joinpath(sortdir, "$name.b.json")
                (isfile(av) && isfile(fa) && isfile(fb)) || continue
                ss = Avro.parseschema(read(av, String))
                @test livecompare("Compare", av, fa, fb) == expectobject       # object-level differences stay recorded
                @test livecompare("CompareBytes", av, fa, fb) == expectbytes  # encoded comparator is the spec oracle
                # Compare the same wire bytes as CompareBytes. Avro's jsontofrag tool waits on some
                # complete JSON strings, and Julia would canonicalise logical surface forms.
                wa = hex2bytes(strip(javaharnesstext(hd, "Encode", av, fa)))
                wb = hex2bytes(strip(javaharnesstext(hd, "Encode", av, fb)))
                if startswith(expectbytes, "ERROR")
                    @test_throws ArgumentError Avro.comparebytes(ss, wa, wb)
                    ran += 1
                    continue
                end
                jv = parse(Int, expectbytes)
                @test sign(jv) == sign(Avro.comparebytes(ss, wa, wb))
                ran += 1
            end
            @test ran == 51                                               # full committed sort surface, both Java comparators
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
    @testset "(7) recorded three-way verdicts for schemas, datums, blocks and JSON" begin
        recorded = negativeverdicts()
        seen = Set{Tuple{String,String}}()
        negative = joinpath(INTEROP_FIXTURES, "negative")
        pynegative = tempname() * ".py"
        write(pynegative, """
import fastavro, io, json, sys
mode, path = sys.argv[1:3]
schemaf = sys.argv[3] if len(sys.argv) > 3 else None
if mode == "schema":
    with open(path, "r", encoding="utf-8") as f:
        fastavro.parse_schema(json.loads(f.read()))
elif mode == "datum":
    with open(schemaf, "r", encoding="utf-8") as f:
        schema = json.load(f)
    with open(path, "r", encoding="ascii") as f:
        stream = io.BytesIO(bytes.fromhex(f.read()))
    fastavro.schemaless_reader(stream, schema)
    if stream.read() != b"":
        raise AssertionError("trailing raw datum bytes")
elif mode == "json":
    with open(schemaf, "r", encoding="utf-8") as f:
        schema = json.load(f)
    with open(path, "r", encoding="utf-8") as f:
        datum = json.loads(f.read())
    fastavro.schemaless_writer(io.BytesIO(), schema, datum)
elif mode == "block":
    with open(path, "rb") as f:
        list(fastavro.reader(f))
else:
    raise AssertionError(f"unknown negative-oracle mode {mode}")
""")
        function pythonverdict(mode::String, path::String, schema::Union{Nothing,String}=nothing)
            cmd = schema === nothing ? `$py $pynegative $mode $path` : `$py $pynegative $mode $path $schema`
            proc = run(pipeline(cmd; stdout=devnull, stderr=devnull); wait=false)
            wait(proc)
            return success(proc)
        end

        function expectedcase(category::String, name::String)
            key = (category, name)
            haskey(recorded, key) || error("no recorded verdict for $category/$name")
            push!(seen, key)
            return recorded[key]
        end

        function blockverdicts(bytes::Vector{UInt8})
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
            return (julia=julia, java=java, fastavro=pythonverdict("block", f))
        end
        s7 = Avro.parseschema("{\"type\":\"record\",\"name\":\"NG\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"boolean\"}]}")
        good7 = take!(Avro.tobuffer([(a=Int64(1), b=true)]; schema=s7))
        entries7 = Avro.Reader(r -> Avro.prescanblocks(r).entries, IOBuffer(good7))
        invalidboolean = copy(good7)
        invalidboolean[entries7[1].offset + 1] = 0x07
        badmagic7 = copy(good7)
        badmagic7[1] = 0x58
        badsync7 = copy(good7)
        badsync7[end] ⊻= 0x01
        blockcases = (("truncated_sync", good7[1:end - 3]), ("invalid_boolean", invalidboolean),
                      ("bad_magic", badmagic7), ("bad_sync", badsync7), ("valid", good7))
        for (name, bytes) in blockcases
            expected = expectedcase("block", name)
            actual = blockverdicts(bytes)
            @test actual == (julia=expected.julia, java=expected.java, fastavro=expected.fastavro)
        end
        schemanames = sort([name for ((category, name), _) in recorded if category == "schema"])
        for name in schemanames
            expected = expectedcase("schema", name)
            path = joinpath(negative, "schema", "$name.avsc")
            julia = try
                Avro.parseschema(read(path, String))
                true
            catch
                false
            end
            java = try
                javatool("canonical", path, "-")
                true
            catch
                false
            end
            @test (julia=julia, java=java, fastavro=pythonverdict("schema", path)) ==
                  (julia=expected.julia, java=expected.java, fastavro=expected.fastavro)
        end
        lavsc7 = joinpath(negative, "datum", "string.avsc")
        ls7 = Avro.parseschema(read(lavsc7, String))
        datumnames = sort([name for ((category, name), _) in recorded if category == "datum"])
        for name in datumnames
            expected = expectedcase("datum", name)
            path = joinpath(negative, "datum", "$name.hex")
            bytes = fixturebytes(path)
            julia = try
                Avro.decode(ls7, bytes)
                true
            catch
                false
            end
            f = tempname()
            write(f, bytes)
            java = try
                javatool("fragtojson", "--schema-file", lavsc7, f)
                true
            catch
                false
            end
            @test (julia=julia, java=java, fastavro=pythonverdict("datum", path, lavsc7)) ==
                  (julia=expected.julia, java=expected.java, fastavro=expected.fastavro)
        end
        jsonnames = sort([name for ((category, name), _) in recorded if category == "json"])
        for name in jsonnames
            expected = expectedcase("json", name)
            path = joinpath(negative, "json", "$name.json")
            julia = try
                Avro.fromjson(ls7, read(path, String))
                true
            catch
                false
            end
            java = try
                javatool("jsontofrag", "--schema-file", lavsc7, path)
                true
            catch
                false
            end
            @test (julia=julia, java=java, fastavro=pythonverdict("json", path, lavsc7)) ==
                  (julia=expected.julia, java=expected.java, fastavro=expected.fastavro)
        end
        @test seen == Set(keys(recorded))                                # every recorded case ran in both directions
    end
end
